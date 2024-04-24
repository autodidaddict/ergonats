package main

import (
	"fmt"

	"github.com/autodidaddict/ergonats"
	"github.com/ergo-services/ergo/etf"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type MyConsumer struct {
	ergonats.PullConsumer
}

func (c *MyConsumer) InitPullConsumer(
	proces *ergonats.PullConsumerProcess,
	args ...etf.Term) (*ergonats.PullConsumerOptions, error) {

	return &ergonats.PullConsumerOptions{
		Connection:         args[0].(*nats.Conn),
		StreamName:         "EVENTS",
		NatsConsumerConfig: jetstream.ConsumerConfig{},
	}, nil
}

func (c *MyConsumer) HandleMessage(_ *ergonats.PullConsumerProcess, msg jetstream.Msg) error {
	_ = msg.Ack()
	fmt.Println("Received message on", msg.Subject())
	return nil
}
