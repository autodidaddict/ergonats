package eventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func writeEvents(conn *nats.Conn,
	streamName string,
	eventSubjectPrefix string,
	jsDomain string,
	events []cloudevents.Event) error {
	var err error

	ctx, cancelF := context.WithTimeout(context.Background(), bucketTimeout)
	defer cancelF()

	var js jetstream.JetStream
	if len(jsDomain) == 0 {
		js, err = jetstream.New(conn)
	} else {
		js, err = jetstream.NewWithDomain(conn, jsDomain)
	}

	if err != nil {
		return err
	}

	_, err = js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", eventSubjectPrefix)},
			})
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	for _, event := range events {
		outSubject := eventSubject(eventSubjectPrefix, event)
		bytes, _ := json.Marshal(event)
		err = conn.Publish(outSubject, bytes)
		if err != nil {
			return err
		}
	}

	return nil
}

func eventSubject(prefix string, event cloudevents.Event) string {
	outSubject := fmt.Sprintf("%s.%s", prefix, event.Type())
	if ext, ok := event.Extensions()[extensionEntityKey]; ok {
		ek := strings.ReplaceAll(ext.(string), ".", "_")
		outSubject = fmt.Sprintf("%s.%s.%s", prefix, ek, event.Type())
	}

	return outSubject
}
