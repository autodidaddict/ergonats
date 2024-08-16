package eventsourcing

import "testing"

func TestEventStreamSubject(t *testing.T) {
	event := NewCloudEvent("test_event", "foo", []byte{1, 2, 3})
	event2 := NewCloudEvent("test_event", "foo_bar", []byte{1, 2, 3})
	event3 := NewCloudEvent("test_event", "foo.bar", []byte{1, 2, 3})

	es1 := eventSubject("$PREFIX.events", event)
	if es1 != "$PREFIX.events.foo.test_event" {
		t.Fatalf("Wrong subject: %s", es1)
	}

	es2 := eventSubject("$PREFIX.events", event2)
	if es2 != "$PREFIX.events.foo_bar.test_event" {
		t.Fatalf("Wrong subject: %s", es2)
	}

	es3 := eventSubject("$PREFIX.events", event3)
	if es2 != "$PREFIX.events.foo_bar.test_event" {
		t.Fatalf("Wrong subject: %s", es3)
	}
}
