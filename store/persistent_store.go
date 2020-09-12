package store

import (
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

type PersistentStoreOption = func(*PersistentStore)

func WithDriver(d Driver) PersistentStoreOption {
	return func(s *PersistentStore) {
		s.driver = d
	}
}

type PersistentStore struct {
	driver Driver
}

func NewPersistentStore(opts ...PersistentStoreOption) (*PersistentStore, error) {
	store := &PersistentStore{}

	for _, opt := range opts {
		opt(store)
	}

	return store, nil
}

func (s *PersistentStore) Append(streamName string, version uint64, events ...*pbMessaging.Event) (stream *Stream, err error) {
	if !s.driver.StreamExists(streamName) {
		if stream, err = s.driver.CreateStream(streamName); err != nil {
			return nil, err
		}
	}

	if stream, err = s.driver.Append(streamName, version, events...); err != nil {
		return nil, err
	}

	return stream, nil
}

func (s *PersistentStore) ReadEventsForward(streamName string, offset uint64, limit uint64) ([]*pbMessaging.Event, error) {
	return s.driver.ReadEventsForward(streamName, offset, limit)
}

func (s *PersistentStore) ReadEventsForwardAsync(streamName string, offset uint64, limit uint64) (eventsCh <-chan *pbMessaging.Event, err error) {
	return s.driver.ReadEventsForwardAsync(streamName, offset, limit)
}
