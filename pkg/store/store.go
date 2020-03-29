package store

import (
	"errors"

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

type driver interface {
	AsyncReaderStore
	SyncReaderStore
	WriteOnlyStore
}

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
func WithDriver(d driver) PersistentStoreOption {
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
	driver      driver
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
	return s.driver.Append(streamName, events...)
}

// ReadStreamEventsForward ...
func (s *PersistentStore) ReadStreamEventsForward(streamName string, offset uint64, limit uint64) ([]*pbMessaging.Event, error) {
	return s.driver.ReadStreamEventsForward(streamName, offset, limit)
}

// ReadStreamEventsForwardAsync ...
func (s *PersistentStore) ReadStreamEventsForwardAsync(streamName string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error) {
	return s.driver.ReadStreamEventsForwardAsync(streamName, offset, limit)
}
