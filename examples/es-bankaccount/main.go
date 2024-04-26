package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ergo-services/ergo"
	"github.com/ergo-services/ergo/gen"
	"github.com/ergo-services/ergo/node"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, _ := nats.Connect("0.0.0.0:4222")
	defer nc.Drain()

	js, _ := jetstream.New(nc)

	_, _ = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     bankStream,
		Subjects: []string{"examples.bank.events.*"},
	})

	logger := slog.Default()

	logger.Info("Starting node")
	node_abc, _ := ergo.StartNode("node_abc@localhost", "cookies", node.Options{})
	logger.Info("Node started")

	accountAggregate := &BankAccountAggregate{}
	p, err := node_abc.Spawn("account_aggregate", gen.ProcessOptions{}, accountAggregate,
		nc,
		logger,
	)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	logger.Info("Spawned Bank account aggregate", slog.Any("pid", p.Info().PID))

	node_abc.Wait()

}
