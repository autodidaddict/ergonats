package eventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func writeEvents(conn *nats.Conn,
	streamName string,
	eventSubjectPrefix string,
	events []cloudevents.Event) error {
	var err error

	ctx, cancelF := context.WithTimeout(context.Background(), bucketTimeout)
	defer cancelF()

	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}

	_, err = js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.*", eventSubjectPrefix)},
			})
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	for _, event := range events {
		bytes, _ := json.Marshal(event)
		err = conn.Publish(fmt.Sprintf("%s.%s", eventSubjectPrefix, event.Type()), bytes)
		if err != nil {
			return err
		}
	}

	return nil
}
