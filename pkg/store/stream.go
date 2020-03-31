package store

import pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"

// Stream ...
type Stream struct {
	Name string

	events []*pbMessaging.Event
}

// NewStream ...
func NewStream(name string, events ...*pbMessaging.Event) *Stream {
	return &Stream{name, events}
}

// Iter ...
func (s Stream) Iter() <-chan *pbMessaging.Event {
	ch := make(chan *pbMessaging.Event)

	for _, e := range s.events {
		ch <- e
	}

	close(ch)
	return ch
}
