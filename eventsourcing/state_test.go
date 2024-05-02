package eventsourcing

import (
	"encoding/json"
	"fmt"
	"testing"
)

// Verify that we can round-trip the interface{} Data field in
// the state envelope
func TestTypedState(t *testing.T) {
	type bankState struct {
		Balance       int
		AccountNumber string
	}

	state := AggregateState{
		Version: 1,
		Key:     "testing",
		Data: bankState{
			Balance:       500,
			AccountNumber: "TESTONE",
		},
	}

	bytes, _ := json.Marshal(state)

	var targetState AggregateState
	_ = json.Unmarshal(bytes, &targetState)

	fmt.Printf("%+v\n", targetState)

	bytes2, _ := json.Marshal(state.Data)
	var bs bankState
	_ = json.Unmarshal(bytes2, &bs)
	fmt.Printf("%+v\n", bs)

}
