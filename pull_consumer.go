package ergonats

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ergo-services/ergo/etf"
	"github.com/ergo-services/ergo/gen"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type PullConsumerBehavior interface {
	gen.ServerBehavior

	InitPullConsumer(process *PullConsumerProcess, args ...etf.Term) (*PullConsumerOptions, error)
	HandleMessage(process *PullConsumerProcess, msg jetstream.Msg) error
}

type PullConsumer struct {
	gen.Server
}

type PullConsumerOptions struct {
	Logger             *slog.Logger
	Connection         *nats.Conn
	StreamName         string
	NatsConsumerConfig jetstream.ConsumerConfig
}

type PullConsumerProcess struct {
	gen.ServerProcess

	options  PullConsumerOptions
	behavior PullConsumerBehavior
}

func (pcp *PullConsumerProcess) Options() *PullConsumerOptions {
	return &pcp.options
}

// gen.Server callbacks

func (c *PullConsumer) Init(
	process *gen.ServerProcess,
	args ...etf.Term) error {

	consumerProcess := &PullConsumerProcess{
		ServerProcess: *process,
	}
	consumerProcess.State = nil

	behavior, ok := process.Behavior().(PullConsumerBehavior)
	if !ok {
		return fmt.Errorf("consumer: not a ConsumerBehavior")
	}
	consumerProcess.behavior = behavior

	consumerOpts, err := behavior.InitPullConsumer(consumerProcess, args...)
	if err != nil {
		return err
	}

	if err := consumerOpts.validate(); err != nil {
		return err
	}
	if consumerOpts.Logger == nil {
		consumerOpts.Logger = slog.Default()
	}

	consumerOpts.Logger.Info("Initializing pull consumer", slog.Any("pid", process.Info().PID),
		slog.String("process_name", process.Name()))

	// Initialize the Nats consumer based on consumerOpts
	go consumerProcess.startPulling()

	consumerProcess.options = *consumerOpts

	process.State = consumerProcess
	return nil
}

func (c *PullConsumer) HandleCall(
	process *gen.ServerProcess,
	from gen.ServerFrom,
	message etf.Term) (etf.Term, gen.ServerStatus) {

	return etf.Atom("ok"), gen.ServerStatusOK
}

func (c *PullConsumer) HandleDirect(
	process *gen.ServerProcess,
	ref etf.Ref, message interface{}) (interface{}, gen.DirectStatus) {

	return nil, fmt.Errorf("unsupported request")
}

func (c *PullConsumer) HandleCast(
	process *gen.ServerProcess,
	message etf.Term) gen.ServerStatus {

	behavior := process.Behavior().(PullConsumerBehavior)
	p := process.State.(*PullConsumerProcess)
	err := behavior.HandleMessage(p, message.(jetstream.Msg))
	if err != nil {
		// dispatch / log error
	}

	return gen.ServerStatusOK
}

func (c *PullConsumer) HandleInfo(
	process *gen.ServerProcess,
	message etf.Term) gen.ServerStatus {
	return gen.ServerStatusOK
}

func (c *PullConsumer) Terminate(
	process *gen.ServerProcess,
	reason string) {

}

func (process *PullConsumerProcess) startPulling() {

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ctx := context.Background()
	nc := process.options.Connection
	streamName := process.options.StreamName
	js, _ := jetstream.New(nc)

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		process.options.Logger.Error("Failed to attach to stream",
			slog.String("stream", streamName),
			slog.Any("error", err),
		)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, process.options.NatsConsumerConfig)
	if err != nil {
		process.options.Logger.Error("Failed to create or locate consumer",
			slog.String("consumer", process.options.NatsConsumerConfig.Name),
			slog.Any("error", err),
		)
	}

	_, _ = cons.Consume(func(msg jetstream.Msg) {
		process.Cast(process.Self(), msg)
	})

}

func (opts PullConsumerOptions) validate() error {
	return nil
}
