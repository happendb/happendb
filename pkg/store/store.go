package store

import "github.com/happendb/happendb/proto/gen/go/messaging"

// ReadOnlyStore ...
type ReadOnlyStore interface {
	ReadEvents(aggregateID string) ([]*messaging.Event, error)
}
