package messaging_test

import (
	"testing"

	"github.com/happendb/happendb/pkg/messaging"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/stretchr/testify/assert"
)

func TestEventStream(t *testing.T) {
	event := &pbMessaging.Event{}
	stream := messaging.NewEventStream("foo")
	stream.Append(event)

	assert.Equal(t, 1, stream.Len())
	assert.Equal(t, "foo", stream.Name())

	for e := range stream.Iter() {
		assert.Equal(t, event, e)
	}
}
