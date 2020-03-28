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
	Append(streamName string, events ...*pbMessaging.Event) error
	ReadStreamEventsForwardAsync(streamName string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error)
}

// ReadOnlyStore ...
type ReadOnlyStore interface {
	ReadStreamEventsForwardAsync(aggregateID string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error)
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

// ReadStreamEventsForwardAsync ...
func (s *PersistentStore) ReadStreamEventsForwardAsync(streamName string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error) {
	return s.driver.ReadStreamEventsForwardAsync(streamName, offset, limit)
}
