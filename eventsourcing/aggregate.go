package eventsourcing

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/autodidaddict/ergonats"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/ergo-services/ergo/etf"
	"github.com/ergo-services/ergo/gen"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type AggregateBehavior interface {
	ergonats.PullConsumerBehavior

	InitAggregate(process *AggregateProcess, args ...etf.Term) (AggregateOptions, error)
	ApplyEvent(process *AggregateProcess, state AggregateState, event cloudevents.Event) (AggregateState, error)
	HandleCommand(process *AggregateProcess, state AggregateState, cmd Command) ([]cloudevents.Event, error)
}

type Aggregate struct {
	ergonats.PullConsumer
}

type AggregateProcess struct {
	ergonats.PullConsumerProcess

	options  AggregateOptions
	behavior AggregateBehavior
}

type AggregateOptions struct {
	Connection           *nats.Conn
	StreamName           string
	AcceptedCommands     []string
	StateStoreBucketName string
	AggregateName        string
}

func (a *Aggregate) InitPullConsumer(
	process *ergonats.PullConsumerProcess,
	args ...etf.Term) (*ergonats.PullConsumerOptions, error) {

	fmt.Println("init pullconsumer")

	aggregateProcess := &AggregateProcess{
		PullConsumerProcess: *process,
	}
	aggregateProcess.State = nil
	behavior, ok := process.Behavior().(AggregateBehavior)
	if !ok {
		return nil, fmt.Errorf("aggregate: not an AggregateBehavior")
	}
	aggregateProcess.behavior = behavior

	aggregateOpts, err := behavior.InitAggregate(aggregateProcess, args...)
	if err != nil {
		return nil, err
	}
	aggregateProcess.options = aggregateOpts
	process.State = aggregateProcess

	for _, cmdType := range aggregateOpts.AcceptedCommands {
		_, err := aggregateOpts.Connection.Subscribe(fmt.Sprintf("ergonats.cmd.%s", cmdType),
			a.handleCommandMessage(aggregateProcess),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to subscribe to command type '%s': %s", cmdType, err)
		}
	}

	consumerName := fmt.Sprintf("AGG_%s", aggregateOpts.AggregateName)

	return &ergonats.PullConsumerOptions{
		Connection: aggregateOpts.Connection,
		StreamName: aggregateOpts.StreamName,
		NatsConsumerConfig: jetstream.ConsumerConfig{
			Durable:     consumerName,
			Name:        consumerName,
			MaxDeliver:  2,
			Description: fmt.Sprintf("Aggregate consumer for %s", aggregateOpts.AggregateName),
		},
	}, nil
}

func (a *Aggregate) handleCommandMessage(p *AggregateProcess) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		tokens := strings.Split(msg.Subject, ".")
		// ergonats.cmd.{type}
		if len(tokens) != 3 {
			reply := CommandReply{
				Accepted: false,
				Message:  "Bad request",
			}
			bytes, _ := json.Marshal(&reply)
			_ = msg.Respond(bytes)
			return
		}
		commandType := tokens[2]

		behavior := p.Behavior().(AggregateBehavior)

		entityKey := msg.Header.Get(headerEntityKey)
		if len(strings.TrimSpace(entityKey)) == 0 {
			reply := CommandReply{
				Accepted: false,
				Message:  "Bad request - no entity key supplied",
			}
			bytes, _ := json.Marshal(&reply)
			_ = msg.Respond(bytes)
			return
		}

		cmd := Command{
			Type: commandType,
			Data: msg.Data,
		}

		existingState, err := LoadState(p.options.Connection, &p.options, entityKey)
		if err != nil {
			fmt.Printf("%+v\n", err)
			reply := CommandReply{
				Accepted: false,
				Message:  "Failed to load aggregate state",
			}
			bytes, _ := json.Marshal(&reply)
			_ = msg.Respond(bytes)
			return
		}

		events, err := behavior.HandleCommand(p, *existingState, cmd)
		if err != nil {
			reply := CommandReply{
				Accepted: false,
				Message:  fmt.Sprintf("Command rejected: %s", err),
			}
			bytes, _ := json.Marshal(&reply)
			_ = msg.Respond(bytes)
			return
		}
		err = a.writeEvents(p, events)
		if err != nil {
			reply := CommandReply{
				Accepted: false,
				Message:  "Event write failure",
			}
			bytes, _ := json.Marshal(&reply)
			_ = msg.Respond(bytes)
		}

		reply := CommandReply{
			Accepted: true,
			Message:  "Command accepted",
		}
		bytes, _ := json.Marshal(&reply)
		_ = msg.Respond(bytes)
	}
}

func (a *Aggregate) HandleMessage(process *ergonats.PullConsumerProcess, msg jetstream.Msg) error {
	fmt.Printf("Receiving message from consumer: %s\n", msg.Subject())
	var event cloudevents.Event
	err := json.Unmarshal(msg.Data(), &event)
	if err != nil {
		fmt.Println(err)
		_ = msg.Nak()
		return gen.ServerStatusOK
	}
	p := process.State.(*AggregateProcess)
	behavior := p.Behavior().(AggregateBehavior)

	entityKey := event.Extensions()[extensionEntityKey].(string)

	existingState, err := LoadState(p.options.Connection, &p.options, entityKey)
	if err != nil {
		fmt.Println(err)
		_ = msg.Nak()
		return gen.ServerStatusOK
	}

	newState, err := behavior.ApplyEvent(p, *existingState, event)
	if err != nil {
		fmt.Println(err)
		_ = msg.Nak()
		return gen.ServerStatusOK
	}

	err = StoreState(p.options.Connection, &p.options, entityKey, newState)
	if err != nil {
		fmt.Println(err)
		_ = msg.Nak()
		return gen.ServerStatusOK
	}

	_ = msg.Ack()
	fmt.Println("Received message on", msg.Subject())
	return nil
}

func (a *Aggregate) writeEvents(process *AggregateProcess, events []cloudevents.Event) error {
	return writeEvents(process.options.Connection, process.options.StreamName, events)
}
