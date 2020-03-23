package messaging

// EventStream ...
type EventStream struct {
	name   string
	events []*Event
}

// NewEventStream ...
func NewEventStream(name string) *EventStream {
	return &EventStream{
		name,
		make([]*Event, 0),
	}
}

// Append ...
func (s *EventStream) Append(e ...*Event) {
	s.events = append(s.events, e...)
}

// Name ...
func (s EventStream) Name() string {
	return s.name
}

// Len ...
func (s EventStream) Len() int {
	return len(s.events)
}

// Events ...
func (s EventStream) Events() chan *Event {
	ch := make(chan *Event)

	go func() {
		defer close(ch)

		for _, e := range s.events {
			ch <- e
		}
	}()

	return ch
}
