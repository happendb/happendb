package store

import (
	"errors"

	messaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

// PersistMode ...
type PersistMode = int

const (
	// PersistModeSingleTable ...
	PersistModeSingleTable PersistMode = iota
)

var (
	// ErrInvalidTableName ...
	ErrInvalidTableName = errors.New("invalid table name")
)

// ReadOnlyStore ...
type ReadOnlyStore interface {
	ReadEvents(aggregateID string) (*messaging.EventStream, error)
}

// WriteOnlyStore ...
type WriteOnlyStore interface {
	Append(streamName string, events ...*messaging.Event) error
}
