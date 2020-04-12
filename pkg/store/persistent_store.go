package store

import (
	"fmt"

	"github.com/happendb/happendb/internal/logtime"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// PersistentStoreOption ...
type PersistentStoreOption = func(*PersistentStore)

// WithDriver ...
func WithDriver(d Driver) PersistentStoreOption {
	return func(s *PersistentStore) {
		s.driver = d
	}
}

// PersistentStore ...
type PersistentStore struct {
	driver Driver
}

// NewPersistentStore ...
func NewPersistentStore(opts ...PersistentStoreOption) (*PersistentStore, error) {
	store := &PersistentStore{}

	for _, opt := range opts {
		opt(store)
	}

	return store, nil
}

// Append ...
func (s *PersistentStore) Append(streamName string, version uint64, events ...*pbMessaging.Event) error {
	defer logtime.Elapsedf("Append")()

	exists, err := s.driver.StreamExists(streamName)

	if err != nil {
		return err
	}

	if !exists {
		if _, err := s.driver.CreateStream(streamName); err != nil {
			return err
		}
	}

	return s.driver.Append(streamName, version, events...)
}

// ReadEventsForward ...
func (s *PersistentStore) ReadEventsForward(streamName string, offset uint64, limit uint64) (events []*pbMessaging.Event, err error) {
	defer logtime.Elapsedf("ReadEventsForward")()

	return s.driver.ReadEventsForward(streamName, offset, limit)
}

// ReadEventsForwardAsync ...
func (s *PersistentStore) ReadEventsForwardAsync(streamName string, offset uint64, limit uint64) (eventsCh <-chan *pbMessaging.Event, err error) {
	defer logtime.Elapsedf("ReadEventsForwardAsync")()

	hasStream, err := s.driver.StreamExists(streamName)

	if err != nil {
		return nil, fmt.Errorf("could not check if stream exists: %v", err)
	}

	if !hasStream {
		if _, err := s.driver.CreateStream(streamName); err != nil {
			return nil, fmt.Errorf("could not create stream: %v", err)
		}
	}

	return s.driver.ReadEventsForwardAsync(streamName, offset, limit)
}
