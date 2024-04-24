package main

const (
	commandCreateAccount = "create_account"
	bankStream           = "BANK_EVENTS"

	commandTypeCreateAccount = "create_account"
	eventTypeAccountCreated  = "account_created"
)

type BankAccountState struct {
	AccountID string `json:"account_id"`
	Balance   uint64 `json:"balance"`
}

type CreateAccountCommand struct {
	AccountID      string `json:"account_id"`
	InitialBalance uint64 `json:"initial_balance"`
}

type AccountCreatedEvent struct {
	AccountID string `json:"account_id"`
	Balance   uint64 `json:"initial_balance"`
}
