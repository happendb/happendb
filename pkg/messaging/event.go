package messaging

import (
	"github.com/golang/protobuf/ptypes/any"
	pb "github.com/happendb/happendb/proto/gen/go/messaging"
)

// Event ...
type Event = pb.Event

// NewEvent ...
func NewEvent() *Event {
	return &Event{
		Payload: &any.Any{},
	}
}
