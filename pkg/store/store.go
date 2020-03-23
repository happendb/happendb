package store

import (
	"errors"

	"github.com/happendb/happendb/pkg/messaging"
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
