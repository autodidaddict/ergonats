package eventsourcing

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type bankState struct {
	Balance       int
	AccountNumber string
}

// Verify that we can round-trip the interface{} Data field in
// the state envelope
func TestTypedState(t *testing.T) {

	shutdown, nc := startNatsServer(t)
	defer shutdown()

	raw, _ := json.Marshal(bankState{
		Balance:       500,
		AccountNumber: "TESTONE",
	})

	state := AggregateState{
		Version: 1,
		Key:     "testing",
		Data:    raw,
	}

	opts := &AggregateOptions{
		Logger:               slog.Default(),
		Connection:           nc,
		ServiceVersion:       "0.0.1",
		CommandSubjectPrefix: "test.cmds",
		EventSubjectPrefix:   "test.events",
		StreamName:           "supertest",
		AcceptedCommands:     []string{},
		StateStoreBucketName: "TEST_SUPER",
		AggregateName:        "testing",
		Middleware:           []AggregateMiddleware{},
	}

	err := StoreState(nc, opts, "TESTONE", state)

	if err != nil {
		t.Fatalf("should have stored state cleanly but didn't: %s", err)
	}

	state2, err := LoadState(nc, opts, "TESTONE")
	if err != nil {
		t.Fatalf("should have loaded state cleanly but didn't: %s", err)
	}

	var bankState2 bankState
	err = json.Unmarshal(state2.Data, &bankState2)
	if err != nil {
		t.Fatalf("Should've unmarshaled state but didn't: %s", err)
	}

	if bankState2.AccountNumber != "TESTONE" {
		t.Fatalf("didn't round trip state properly: %+v", bankState2)
	}

	err = DeleteState(nc, opts, "TESTONE")
	if err != nil {
		t.Fatalf("couldn't delete state properly: %s", err)
	}

	state3, err := LoadState(nc, opts, "TESTONE")
	if err != nil {
		t.Fatalf("shouldn't have gotten an error retrieving non-existent state: %s", err)
	}
	if state3.Version != 0 || state3.Data != nil {
		t.Fatalf("didn't get an empty state: %+v", state3)
	}

}

func startNatsServer(t *testing.T) (func(), *nats.Conn) {
	t.Helper()
	opts := &server.Options{
		JetStream: true,
		Port:      -1,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		server.PrintAndDie("nats-server: " + err.Error())
	}
	s.ConfigureLogger()
	if err := server.Run(s); err != nil {
		server.PrintAndDie(err.Error())
	}

	go s.WaitForShutdown()
	nc, _ := nats.Connect(s.ClientURL())
	return s.Shutdown, nc
}
