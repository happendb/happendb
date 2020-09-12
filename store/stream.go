package store

import pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"

type Stream struct {
	Name           string
	CurrentVersion uint64

	Events []*pbMessaging.Event
}

func NewStream(name string, events ...*pbMessaging.Event) *Stream {
	var currentVersion uint64

	if len(events) > 0 {
		currentVersion = events[len(events)-1].GetVersion()
	}

	return &Stream{name, currentVersion, events}
}

func (s *Stream) Append(_ uint64, events ...*pbMessaging.Event) {
	s.Events = append(s.Events, events...)
	s.CurrentVersion += uint64(len(events))
}

func (s Stream) Empty() bool {
	return len(s.Events) == 0
}

func (s *Stream) AsyncIter() <-chan *pbMessaging.Event {
	ch := make(chan *pbMessaging.Event, len(s.Events))
	defer close(ch)

	for _, e := range s.Events {
		ch <- e
	}

	return ch
}
