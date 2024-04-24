# Ergonats
Ergonats is a set of server processes and miscellaneous tools that make it easy to build [ergo](https://github.com/ergo-services/ergo)-based NATS applications.

Ergo provides an Erlang/OTP-style supervision hierarchy to Go applications. It's even binary-compatible with existing Erlang/Elixir clusters over the network. If you find yourself constantly creating goroutines that have an infinite `for` loop which in turn has a `select` in it, then you're already in a place where you can realize the benefit of Ergo.

To get a good idea for the inspiration behind Ergo, you might want to read Elixir's [GenServer](https://hexdocs.pm/elixir/GenServer.html) documentation, or you can read the Erlang documentation on [OTP](https://www.erlang.org/doc/design_principles/des_princ).

Ergonats contains some general purpose NATS-related servers like the [PullConsumer](./pull_consumer.go) as well as a complete [event sourcing](./eventsourcing/) library built on top of Ergo.