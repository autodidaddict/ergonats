package main

import (
	"context"
	"fmt"
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

	_, _ = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     bankStream,
		Subjects: []string{"ergonats.events.*"},
	})

	// js.Publish(ctx, "events.1", nil)
	// js.Publish(ctx, "events.2", nil)
	// js.Publish(ctx, "events.3", nil)

	fmt.Println("Starting node")
	node_abc, _ := ergo.StartNode("node_abc@localhost", "cookies", node.Options{})
	fmt.Println("Node started")

	accountAggregate := &BankAccountAggregate{}
	p, err := node_abc.Spawn("account_aggregate", gen.ProcessOptions{}, accountAggregate,
		nc,
	)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	fmt.Printf("Spawned Bank account aggregate, pid %s\n", p.Info().PID)

	node_abc.Wait()

}
