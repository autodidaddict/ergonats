package main

const (
	bankStream = "BANK_EVENTS"

	commandTypeCreateAccount = "create_account"
	commandTypeDeposit       = "deposit"

	eventTypeAccountCreated = "account_created"
	eventTypeFundsDeposited = "funds_deposited"
)

type BankAccountState struct {
	AccountID string `json:"account_id"`
	Balance   uint64 `json:"balance"`
}

type CreateAccountCommand struct {
	AccountID      string `json:"account_id"`
	InitialBalance uint64 `json:"initial_balance"`
}

type DepositFundsCommand struct {
	AccountID string `json:"account_id"`
	Amount    uint64 `json:"amount"`
}

type FundsDepositedEvent struct {
	AccountID string `json:"account_id"`
	Amount    uint64 `json:"amount"`
}

type AccountCreatedEvent struct {
	AccountID string `json:"account_id"`
	Balance   uint64 `json:"initial_balance"`
}
