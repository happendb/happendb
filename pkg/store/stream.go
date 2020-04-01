package store

import pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"

// Stream ...
type Stream struct {
	Name string

	events         []*pbMessaging.Event
	currentVersion uint64
}

// NewStream ...
func NewStream(name string, events ...*pbMessaging.Event) *Stream {
	var currentVersion uint64

	if len(events) > 0 {
		currentVersion = events[len(events)-1].GetVersion()
	}

	return &Stream{name, events, currentVersion}
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
