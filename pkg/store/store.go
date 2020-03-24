package store

import (
	"errors"

	"github.com/happendb/happendb/pkg/messaging"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// Driver ...
type Driver interface {
}

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
	ReadEvents(aggregateID string) (*messaging.EventStream, error)
}

// ReadOnlyStore ...
type ReadOnlyStore interface {
	ReadEvents(aggregateID string) (*messaging.EventStream, error)
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

// ReadEvents ...
func (s *PersistentStore) ReadEvents(aggregateID string) (*messaging.EventStream, error) {
	return s.driver.ReadEvents(aggregateID)
}
