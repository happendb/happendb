package messaging

import (
	pb "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// EventStream ...
type EventStream struct {
	*pb.EventStream
}

// NewEventStream ...
func NewEventStream(name string) *EventStream {
	return &EventStream{
		EventStream: &pb.EventStream{
			Name:   name,
			Events: make([]*pb.Event, 0),
		},
	}
}

// Append ...
func (s *EventStream) Append(e ...*pb.Event) {
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

// Events ...
func (s EventStream) Events() chan *pb.Event {
	ch := make(chan *pb.Event)

	go func() {
		defer close(ch)

		for _, e := range s.EventStream.Events {
			ch <- e
		}
	}()

	return ch
}
