package messaging

import (
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// Event ...
type Event struct {
	*pbMessaging.Event
}

// WrapN ...
func WrapN(protoEvents []*pbMessaging.Event) []*Event {
	events := make([]*Event, 0)

	for _, e := range protoEvents {
		events = append(events, &Event{e})
	}

	return events
}
