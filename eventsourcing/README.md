# Ergonats Event Sourcing
Ergonats provides a number of building block components for creating an event-sourced application
that works seamlessly within an **Ergo** supervision tree.

## Aggregates
An aggregate validates and potentially rejects command requests. If the request passes validation, then the aggregate can emit events to the stream.

An aggregate determines its own _internal_ state by sequentially applying the appropriate events. 

To create your own aggregate, all you need to do is create an aggregate struct:

```go
type BankAccountAggregate struct {
	es.Aggregate
}
```

and then implement the aggregate behavior interface:

```go
InitAggregate(process *AggregateProcess, args ...etf.Term) (AggregateOptions, error)
	ApplyEvent(process *AggregateProcess, state AggregateState, event cloudevents.Event) (AggregateState, error)
	HandleCommand(process *AggregateProcess, state AggregateState, cmd Command) ([]cloudevents.Event, error)
```

* `InitAggregate` - parameters are passed to the process during the init phase. 
* `ApplyEvent` - given an existing state and a cloud event, returns a new state generation
* `HandleCommand` - given an existing state and a command request, returns either an error or a list of events to be emitted.