package store

import (
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

// WithPersistMode ...
func WithPersistMode(m PersistMode) PersistentStoreOption {
	return func(s *PersistentStore) {
		s.persistMode = m
	}
}

// PersistentStore ...
type PersistentStore struct {
	driver      Driver
	persistMode PersistMode
}

// NewPersistentStore ...
func NewPersistentStore(opts ...PersistentStoreOption) (*PersistentStore, error) {
	store := &PersistentStore{
		persistMode: PersistModeSingleTable,
	}

	for _, opt := range opts {
		opt(store)
	}

	return store, nil
}

// Append ...
func (s *PersistentStore) Append(streamName string, events ...*pbMessaging.Event) error {
	defer logtime.Elapsedf("%T::Append", s)()

	exists, err := s.driver.HasStream(streamName)

	if err != nil {
		return err
	}

	if !exists {
		if _, err := s.driver.CreateStream(streamName); err != nil {
			return err
		}
	}

	return s.driver.Append(streamName, events...)
}

// ReadEventsForward ...
func (s *PersistentStore) ReadEventsForward(streamName string, offset uint64, limit uint64) (events []*pbMessaging.Event, err error) {
	defer logtime.Elapsedf("%T::ReadEventsForward", s)()

	events, err = s.driver.ReadEventsForward(streamName, offset, limit)

	return
}

// ReadEventsForwardAsync ...
func (s *PersistentStore) ReadEventsForwardAsync(streamName string, offset uint64, limit uint64) (eventsCh <-chan *pbMessaging.Event, err error) {
	defer logtime.Elapsedf("%T::ReadEventsForwardAsync", s)()

	eventsCh, err = s.driver.ReadEventsForwardAsync(streamName, offset, limit)

	return
}
