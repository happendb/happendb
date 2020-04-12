package driver

import (
	"fmt"

	"github.com/happendb/happendb/pkg/store"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// MemoryDriver ...
type MemoryDriver struct {
	Streams map[string][]*pbMessaging.Event
}

// NewMemoryDriver ...
func NewMemoryDriver() (*MemoryDriver, error) {
	return &MemoryDriver{Streams: make(map[string][]*pbMessaging.Event, 0)}, nil
}

// ReadEventsForward ...
func (d *MemoryDriver) ReadEventsForward(aggregateID string, offset uint64, limit uint64) ([]*pbMessaging.Event, error) {
	return d.Streams[d.generateTableName(aggregateID)], nil
}

// ReadEventsForwardAsync ...
func (d *MemoryDriver) ReadEventsForwardAsync(aggregateID string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error) {
	eventsCh := make(chan *pbMessaging.Event, 0)

	fmt.Printf("ch %+v\n", eventsCh)
	go func() {
		for _, event := range d.Streams[d.generateTableName(aggregateID)] {
			eventsCh <- event
		}

		close(eventsCh)
	}()

	return eventsCh, nil
}

// Append ...
func (d *MemoryDriver) Append(streamName string, version uint64, events ...*pbMessaging.Event) error {
	d.Streams[streamName] = append(d.Streams[streamName], events...)
	return nil
}

// CreateStream ...
func (d *MemoryDriver) CreateStream(name string) (*store.Stream, error) {
	d.Streams[name] = make([]*pbMessaging.Event, 0)
	return store.NewStream(name, d.Streams[name]...), nil
}

// StreamExists ...
func (d *MemoryDriver) StreamExists(name string) (bool, error) {
	_, ok := d.Streams[name]
	return ok, nil
}

// DeleteStream ...
func (d *MemoryDriver) DeleteStream(name string) error {
	panic("DeleteStream() unimplemented")
}

func (d *MemoryDriver) generateTableName(streamName string) string {
	return fmt.Sprintf("events_%s", streamName)
}
