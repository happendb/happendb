package store

import (
	"errors"

	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
)

var (
	ErrInvalidStreamName = errors.New("invalid stream name")
)

type AsyncReader interface {
	ReadEventsForwardAsync(streamName string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error)
}

type SyncReader interface {
	ReadEventsForward(streamName string, offset uint64, limit uint64) ([]*pbMessaging.Event, error)
}

type Reader interface {
	AsyncReader
	SyncReader
}

type Writer interface {
	Append(streamName string, version uint64, events ...*pbMessaging.Event) (stream *Stream, err error)
}
