package eventsourcing

import (
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/google/uuid"
)

const (
	headerEntityKey    = "x-ergonats-entity-key"
	extensionEntityKey = "entitykey"
)

type AggregateState struct {
	Version uint64      `json:"version"`
	Key     string      `json:"key"`
	Data    interface{} `json:"data,omitempty"`
}

type Command struct {
	Type string `json:"type"`
	Data []byte `json:"-"`
}

type CommandReply struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message"`
}

func NewCloudEvent(eventType string, entityKey string, rawData interface{}) cloudevents.Event {
	cloudevent := cloudevents.NewEvent()
	cloudevent.SetSource("ergonats")
	cloudevent.SetExtension(extensionEntityKey, entityKey)
	cloudevent.SetID(uuid.NewString())
	cloudevent.SetTime(time.Now().UTC())
	cloudevent.SetType(eventType)
	cloudevent.SetDataContentType(cloudevents.ApplicationJSON)
	_ = cloudevent.SetData(rawData)

	return cloudevent

}
