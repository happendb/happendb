package messaging

import (
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// EventStream ...
type EventStream struct {
	*pbMessaging.EventStream
}

// NewEventStream ...
func NewEventStream(name string, events ...*pbMessaging.Event) *EventStream {
	return &EventStream{
		EventStream: &pbMessaging.EventStream{
			Name:   name,
			Events: append(make([]*pbMessaging.Event, 0), events...),
		},
	}
}

// Append ...
func (s *EventStream) Append(e ...*pbMessaging.Event) {
	s.EventStream.Events = append(s.EventStream.Events, e...)
}

// Name ...
func (s EventStream) Name() string {
	return s.EventStream.Name
}

// Len ...
func (s EventStream) Len() int {
	return len(s.EventStream.Events)
}

// Iter ...
func (s *EventStream) Iter() <-chan *pbMessaging.Event {
	ch := make(chan *pbMessaging.Event)

	go func() {
		defer close(ch)

		for _, e := range s.EventStream.Events {
			ch <- e
		}
	}()

	return ch
}
