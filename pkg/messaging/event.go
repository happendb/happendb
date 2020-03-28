package messaging

import (
	"github.com/golang/protobuf/ptypes/any"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// Event ...
type Event struct {
	*pbMessaging.Event
}

// NewEvent ...
func NewEvent(payload *any.Any, metadata *any.Any) *Event {
	return &Event{
		&pbMessaging.Event{
			Payload:  payload,
			Metadata: metadata,
		},
	}
}

// WrapN ...
func WrapN(protoEvents []*pbMessaging.Event) []*Event {
	events := make([]*Event, 0)

	for _, e := range protoEvents {
		events = append(events, &Event{e})
	}

	return events
}

// UnwrapN ...
func UnwrapN(events []*Event) []*pbMessaging.Event {
	protoEvents := make([]*pbMessaging.Event, 0)

	for _, e := range events {
		protoEvents = append(protoEvents, e.Event)
	}

	return protoEvents
}
