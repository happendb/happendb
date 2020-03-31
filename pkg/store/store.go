package store

import (
	"errors"

	"github.com/happendb/happendb/internal/logtime"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// PersistMode ...
type PersistMode = byte

const (
	// PersistModeSingleTable ...
	PersistModeSingleTable PersistMode = iota
)

var (
	// ErrInvalidTableName ...
	ErrInvalidTableName = errors.New("invalid table name")
)

// AsyncReaderStore ...
type AsyncReaderStore interface {
	ReadStreamEventsForwardAsync(streamName string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error)
}

// SyncReaderStore ...
type SyncReaderStore interface {
	ReadStreamEventsForward(streamName string, offset uint64, limit uint64) ([]*pbMessaging.Event, error)
}

// ReaderStore ...
type ReaderStore interface {
	AsyncReaderStore
	SyncReaderStore
}

// WriteOnlyStore ...
type WriteOnlyStore interface {
	Append(streamName string, events ...*pbMessaging.Event) error
}

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

// ReadStreamEventsForward ...
func (s *PersistentStore) ReadStreamEventsForward(streamName string, offset uint64, limit uint64) (events []*pbMessaging.Event, err error) {
	defer logtime.Elapsedf("%T::ReadStreamEventsForward", s)()

	events, err = s.driver.ReadStreamEventsForward(streamName, offset, limit)

	return
}

// ReadStreamEventsForwardAsync ...
func (s *PersistentStore) ReadStreamEventsForwardAsync(streamName string, offset uint64, limit uint64) (eventsCh <-chan *pbMessaging.Event, err error) {
	defer logtime.Elapsedf("%T::ReadStreamEventsForwardAsync", s)()

	eventsCh, err = s.driver.ReadStreamEventsForwardAsync(streamName, offset, limit)

	return
}
