package store

import (
	"errors"

	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

var (
	// ErrInvalidTableName ...
	ErrInvalidTableName = errors.New("invalid table name")
	// ErrInvalidStreamName ...
	ErrInvalidStreamName = errors.New("invalid stream name")
	// ErrExpectedVersion ...
	ErrExpectedVersion = errors.New("expected version should be current version + 1")
)

// AsyncReaderStore ...
type AsyncReaderStore interface {
	ReadEventsForwardAsync(streamName string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error)
}

// SyncReaderStore ...
type SyncReaderStore interface {
	ReadEventsForward(streamName string, offset uint64, limit uint64) ([]*pbMessaging.Event, error)
}

// ReaderStore ...
type ReaderStore interface {
	AsyncReaderStore
	SyncReaderStore
}

// WriteOnlyStore ...
type WriteOnlyStore interface {
	Append(streamName string, version uint64, events ...*pbMessaging.Event) error
}
