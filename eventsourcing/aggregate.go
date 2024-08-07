package eventsourcing

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/autodidaddict/ergonats"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/ergo-services/ergo/etf"
	"github.com/ergo-services/ergo/gen"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

type AggregateBehavior interface {
	ergonats.PullConsumerBehavior

	InitAggregate(process *AggregateProcess, args ...etf.Term) (AggregateOptions, error)
	ApplyEvent(process *AggregateProcess, state AggregateState, event cloudevents.Event) (*AggregateState, error)
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
	Logger                 *slog.Logger
	Connection             *nats.Conn
	JsDomain               string
	ServiceVersion         string
	CommandSubjectPrefix   string
	EventSubjectPrefix     string
	StreamName             string
	AcceptedCommands       []string
	StateStoreBucketName   string
	StateStoreMaxValueSize int
	StateStoreMaxBytes     int
	AggregateName          string
	Middleware             []AggregateMiddleware
}

type AggregateMiddleware interface {
	ExecMiddleware(*AggregateState, *Command) error
}

func (a *Aggregate) InitPullConsumer(
	process *ergonats.PullConsumerProcess,
	args ...etf.Term) (*ergonats.PullConsumerOptions, error) {

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
	if aggregateOpts.ServiceVersion == "" {
		aggregateOpts.ServiceVersion = "0.0.1"
	}

	// Set these to unlimited (-1) if not specified
	if aggregateOpts.StateStoreMaxBytes == 0 {
		aggregateOpts.StateStoreMaxBytes = -1
	}
	if aggregateOpts.StateStoreMaxValueSize == 0 {
		aggregateOpts.StateStoreMaxValueSize = -1
	}

	if aggregateOpts.Logger == nil {
		aggregateOpts.Logger = slog.Default()
	}
	aggregateProcess.options = aggregateOpts
	process.State = aggregateProcess

	s, err := micro.AddService(aggregateOpts.Connection, micro.Config{
		Name:        fmt.Sprintf("aggregate-%s", aggregateOpts.AggregateName),
		Version:     aggregateOpts.ServiceVersion,
		Description: fmt.Sprintf("%s Aggregate Service", aggregateOpts.AggregateName),
		QueueGroup:  aggregateOpts.AggregateName,
	})
	if err != nil {
		return nil, gen.ServerStatusStop
	}
	tokens := strings.Split(aggregateOpts.CommandSubjectPrefix, ".")
	var g micro.Group
	g = s.AddGroup(tokens[0])
	for _, token := range tokens[1:] {
		g = g.AddGroup(token)
	}

	for _, cmdType := range aggregateOpts.AcceptedCommands {
		g.AddEndpoint(cmdType, micro.HandlerFunc(a.handleCommandMessage(cmdType, aggregateProcess)))
	}

	consumerName := fmt.Sprintf("AGG_%s", aggregateOpts.AggregateName)

	aggregateOpts.Logger.Info("Aggregate initialized", slog.String("name", aggregateOpts.AggregateName))
	return &ergonats.PullConsumerOptions{
		Logger:     aggregateOpts.Logger,
		Connection: aggregateOpts.Connection,
		JsDomain:   aggregateOpts.JsDomain,
		StreamName: aggregateOpts.StreamName,
		NatsConsumerConfig: jetstream.ConsumerConfig{
			Durable:     consumerName,
			Name:        consumerName,
			MaxDeliver:  2,
			Description: fmt.Sprintf("Aggregate consumer for %s", aggregateOpts.AggregateName),
		},
	}, nil
}

func (a *Aggregate) handleCommandMessage(commandType string, p *AggregateProcess) func(micro.Request) {
	return func(request micro.Request) {
		behavior := p.Behavior().(AggregateBehavior)

		entityKey := request.Headers().Get(headerEntityKey)
		if len(strings.TrimSpace(entityKey)) == 0 {
			reply := CommandReply{
				Accepted: false,
				Message:  "No entity key supplied",
			}
			bytes, _ := json.Marshal(&reply)
			_ = request.Error("400", reply.Message, bytes)
			return
		}

		cmd := Command{
			Type:     commandType,
			Data:     request.Data(),
			Metadata: make(map[string]string),
		}

		hdr := request.Headers()
		h := map[string][]string(hdr)
		for k, v := range h {
			cmd.Metadata[k] = v[0]
		}

		existingState, err := LoadState(p.options.Connection, &p.options, entityKey)
		if err != nil {
			p.options.Logger.Error("Failed to load aggregate state", slog.Any("error", err))
			reply := CommandReply{
				Accepted: false,
				Message:  "Failed to load aggregate state",
			}
			bytes, _ := json.Marshal(&reply)
			_ = request.Error("500", reply.Message, bytes)
			return
		}

		err = runMiddleware(p.options.Middleware, existingState, &cmd)
		if err != nil {
			p.options.Logger.Error("Middleware execution failed", slog.Any("error", err))
			reply := CommandReply{
				Accepted: false,
				Message:  err.Error(),
			}
			bytes, _ := json.Marshal(&reply)
			_ = request.Error("500", reply.Message, bytes)
			return
		}

		events, err := behavior.HandleCommand(p, *existingState, cmd)
		if err != nil {
			reply := CommandReply{
				Accepted: false,
				Message:  fmt.Sprintf("Command rejected: %s", err),
			}
			bytes, _ := json.Marshal(&reply)
			_ = request.Error("400", reply.Message, bytes)
			return
		}
		err = a.writeEvents(p, events)
		if err != nil {
			reply := CommandReply{
				Accepted: false,
				Message:  "Event write failure",
			}
			bytes, _ := json.Marshal(&reply)
			_ = request.Error("500", reply.Message, bytes)
		}

		reply := CommandReply{
			Accepted: true,
			Message:  "Command accepted",
		}
		bytes, _ := json.Marshal(&reply)
		_ = request.Respond(bytes)
	}
}

func (a *Aggregate) HandleMessage(process *ergonats.PullConsumerProcess, msg jetstream.Msg) error {
	popts := process.Options()
	ap := process.State.(*AggregateProcess)
	aopts := ap.options

	popts.Logger.Info("Aggregate receiving message from consumer",
		slog.String("aggregate", aopts.AggregateName),
		slog.String("subject", msg.Subject()),
	)

	p := process.State.(*AggregateProcess)
	behavior := p.Behavior().(AggregateBehavior)

	var event cloudevents.Event
	err := json.Unmarshal(msg.Data(), &event)
	if err != nil {
		popts.Logger.Error("Failed to unmarshal cloud event", slog.Any("error", err))
		_ = msg.Nak()
		return gen.ServerStatusOK
	}

	entityKey := event.Extensions()[extensionEntityKey].(string)

	existingState, err := LoadState(p.options.Connection, &p.options, entityKey)
	if err != nil {
		popts.Logger.Error("Failed to load state",
			slog.Any("error", err),
			slog.String("entity_key", entityKey),
		)
		_ = msg.Nak()
		return gen.ServerStatusOK
	}

	newState, err := behavior.ApplyEvent(p, *existingState, event)
	if err != nil {
		popts.Logger.Error("Failed to apply event",
			slog.Any("error", err),
			slog.String("event", event.Type()),
			slog.String("entity_key", entityKey),
		)
		_ = msg.Nak()
		return gen.ServerStatusOK
	}
	// check if the aggregate requested a delete
	if newState == nil {
		popts.Logger.Info("Deleting aggregate", slog.String("key", entityKey))
		err = DeleteState(p.options.Connection, &p.options, entityKey)
		if err != nil {
			popts.Logger.Error("Failed to delete state",
				slog.Any("error", err),
				slog.String("entity_key", entityKey),
			)
			_ = msg.Nak()
			return gen.ServerStatusOK
		}
	} else {
		err = StoreState(p.options.Connection, &p.options, entityKey, *newState)
		if err != nil {
			popts.Logger.Error("Failed to store state",
				slog.Any("error", err),
				slog.String("entity_key", entityKey),
			)
			_ = msg.Nak()
			return gen.ServerStatusOK
		}
	}

	_ = msg.Ack()
	return nil
}

func (a *Aggregate) writeEvents(process *AggregateProcess, events []cloudevents.Event) error {
	return writeEvents(process.options.Connection,
		process.options.StreamName,
		process.options.EventSubjectPrefix,
		process.options.JsDomain,
		events)
}

func runMiddleware(middlewares []AggregateMiddleware, state *AggregateState, cmd *Command) error {
	if middlewares == nil {
		return nil
	}
	// in this chain, state and command can both be modified

	for _, mw := range middlewares {
		err := mw.ExecMiddleware(state, cmd)
		if err != nil {
			return err
		}
	}

	return nil

}
