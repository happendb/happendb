package driver

import (
	"fmt"

	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/happendb/happendb/store"
)

type MemoryDriver struct {
	Streams map[string]*store.Stream
}

func NewMemoryDriver() (*MemoryDriver, error) {
	return &MemoryDriver{Streams: make(map[string]*store.Stream, 0)}, nil
}

func (d *MemoryDriver) Append(streamName string, version uint64, events ...*pbMessaging.Event) (stream *store.Stream, err error) {
	stream = d.Streams[d.generateTableName(streamName)]
	stream.Append(version, events...)
	return stream, nil
}

func (d *MemoryDriver) ReadEventsForward(streamName string, offset uint64, limit uint64) ([]*pbMessaging.Event, error) {
	var (
		stream *store.Stream
		events []*pbMessaging.Event
	)

	var ok bool
	if stream, ok = d.Streams[d.generateTableName(streamName)]; !ok {
		return nil, nil
	}

	if !stream.Empty() {
		for _, event := range stream.Events {
			events = append(events, event)
		}
	}

	return events, nil
}

func (d *MemoryDriver) ReadEventsForwardAsync(aggregateID string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error) {
	streamName := d.generateTableName(aggregateID)
	var stream *store.Stream

	var ok bool
	if stream, ok = d.Streams[streamName]; !ok {
		return nil, nil
	}

	eventsCh := make(chan *pbMessaging.Event, len(stream.Events))

	go func() {
		for event := range stream.AsyncIter() {
			eventsCh <- event
		}

		close(eventsCh)
	}()

	return eventsCh, nil
}

func (d *MemoryDriver) CreateStream(name string) (*store.Stream, error) {
	name = d.generateTableName(name)
	stream := store.NewStream(name)

	d.Streams[name] = stream

	return stream, nil
}

func (d *MemoryDriver) GetStream(name string) *store.Stream {
	if d.StreamExists(name) {
		return d.Streams[name]
	}

	return nil
}

func (d *MemoryDriver) StreamExists(name string) bool {
	return d.Streams[d.generateTableName(name)] != nil
}

func (d *MemoryDriver) DeleteStream(name string) error {
	panic("DeleteStream() unimplemented")
}

func (d *MemoryDriver) generateTableName(streamName string) string {
	return fmt.Sprintf("events_%s", streamName)
}
