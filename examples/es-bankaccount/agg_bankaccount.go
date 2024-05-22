package main

import (
	"encoding/json"
	"errors"
	"log/slog"

	es "github.com/autodidaddict/ergonats/eventsourcing"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/ergo-services/ergo/etf"
	"github.com/nats-io/nats.go"
)

type BankAccountAggregate struct {
	es.Aggregate
}

func (b *BankAccountAggregate) InitAggregate(
	process *es.AggregateProcess,
	args ...etf.Term) (es.AggregateOptions, error) {

	var logger *slog.Logger
	if len(args) < 2 {
		logger = slog.Default()
	} else {
		logger = args[1].(*slog.Logger)
	}

	logger.Info("Initializing bank account aggregate")

	return es.AggregateOptions{
		Connection:     args[0].(*nats.Conn),
		Logger:         logger,
		StreamName:     bankStream,
		ServiceVersion: "0.1.0",
		AcceptedCommands: []string{
			commandTypeCreateAccount,
			commandTypeDeposit,
		},
		CommandSubjectPrefix: "examples.bank.cmds",
		EventSubjectPrefix:   "examples.bank.events",
		StateStoreBucketName: "AGG_bankaccount",
		AggregateName:        "bankaccount",
		Middleware: []es.AggregateMiddleware{
			authenticator{},
		},
	}, nil
}

func (b *BankAccountAggregate) ApplyEvent(
	process *es.AggregateProcess,
	state es.AggregateState,
	event cloudevents.Event) (es.AggregateState, error) {

	switch event.Type() {
	case eventTypeAccountCreated:
		var evt AccountCreatedEvent
		err := event.DataAs(&evt)
		if err != nil {
			return state, err
		}
		// Note that state.Key is not modified here as that comes from
		// the header on the stored message/event.

		// WARNING: if the value of x-ergonats-entity-key doesn't match _exactly_ the
		// AccountID below, your app won't behave the way you expect
		raw, _ := json.Marshal(BankAccountState{
			AccountID: evt.AccountID,
			Balance:   evt.Balance,
		})
		state.Data = raw
	case eventTypeFundsDeposited:
		var evt FundsDepositedEvent
		err := event.DataAs(&evt)
		if err != nil {
			return state, err
		}
		var bankState BankAccountState
		err = json.Unmarshal(state.Data, &bankState)
		if err != nil {
			return state, err
		}
		bankState.Balance += evt.Amount
		raw, _ := json.Marshal(bankState)

		state.Data = raw
	}

	return state, nil
}

func (b *BankAccountAggregate) HandleCommand(
	process *es.AggregateProcess,
	state es.AggregateState,
	cmd es.Command) ([]cloudevents.Event, error) {

	switch cmd.Type {
	case commandTypeCreateAccount:
		return createAccount(cmd, &state)
	case commandTypeDeposit:
		return deposit(cmd, state)
	default:
		return nil, errors.New("unexpected command type")
	}
}

func deposit(cmd es.Command, state es.AggregateState) ([]cloudevents.Event, error) {
	if state.Version == 0 {
		return []cloudevents.Event{}, errors.New("can't deposit into a non-existent account")
	}

	var depositCommand DepositFundsCommand
	err := json.Unmarshal(cmd.Data, &depositCommand)
	if err != nil {
		return nil, err
	}

	fundsDeposited := FundsDepositedEvent{
		AccountID: depositCommand.AccountID,
		Amount:    depositCommand.Amount,
	}

	return []cloudevents.Event{
		es.NewCloudEvent(eventTypeFundsDeposited, depositCommand.AccountID, fundsDeposited),
	}, nil
}

func createAccount(cmd es.Command, state *es.AggregateState) ([]cloudevents.Event, error) {
	var createCommand CreateAccountCommand
	err := json.Unmarshal(cmd.Data, &createCommand)
	if err != nil {
		return nil, err
	}

	if state.Version > 0 {
		return []cloudevents.Event{}, errors.New("can't create an account that already has previous events")
	}

	if createCommand.InitialBalance < 100 {
		return []cloudevents.Event{}, errors.New("bank accounts must be created with at least 100 moneybucks")
	}

	accountCreated := AccountCreatedEvent{
		AccountID: createCommand.AccountID,
		Balance:   createCommand.InitialBalance,
	}

	return []cloudevents.Event{
		es.NewCloudEvent(eventTypeAccountCreated, createCommand.AccountID, accountCreated),
	}, nil
}

type authenticator struct{}

func (a authenticator) ExecMiddleware(state *es.AggregateState, cmd *es.Command) error {
	username, ok := cmd.Metadata["x-username"]
	if !ok {
		return errors.New("username must be supplied")
	}
	if username == "unauthorized" {
		return errors.New("unauthorized user")
	}

	return nil
}
