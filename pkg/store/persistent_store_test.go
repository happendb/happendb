package store_test

import (
	"fmt"
	"testing"

	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/driver"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/stretchr/testify/assert"
)

func MakeDriver(t *testing.T) *driver.MemoryDriver {
	d, err := driver.NewMemoryDriver()
	assert.NoError(t, err)

	return d
}

func TestAppend(t *testing.T) {
	tests := []struct {
		name          string
		expectedError error
	}{
		{name: "AppendOk"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, err := store.NewPersistentStore(store.WithDriver(MakeDriver(t)))
			assert.NoError(t, err)

			err = st.Append("my-stream", 1, []*pbMessaging.Event{
				{
					Id: "ff333f0d-447e-4579-8656-a48fb30ea120",
				},
				{
					Id: "bbea682c-c503-487f-88da-40d128b2318e",
				},
				{
					Id: "82e2bb27-6d3a-48d6-8174-27b2f3b3c23f",
				},
			}...)

			assert.Equal(t, tt.expectedError, err)
		})
	}
}

func TestReadEventsForward(t *testing.T) {
	tests := []struct {
		name          string
		streamName    string
		expectedError error
	}{
		{"ReadEventsForwardOk", "my-stream", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := MakeDriver(t)
			st, err := store.NewPersistentStore(store.WithDriver(d))
			assert.NoError(t, err)

			name := fmt.Sprintf("events_%s", tt.streamName)

			d.Streams[name] = append(d.Streams[name], &pbMessaging.Event{Id: "1"}, &pbMessaging.Event{Id: "2"}, &pbMessaging.Event{Id: "3"})

			events, err := st.ReadEventsForward(tt.streamName, 0, 3)

			assert.Equal(t, tt.expectedError, err)
			assert.Len(t, events, 3)

			for i := 0; i < len(events); i++ {
				assert.Equal(t, events[i].Id, fmt.Sprintf("%v", i+1))
			}
		})
	}
}

func TestReadEventsForwardAsync(t *testing.T) {
	tests := []struct {
		name          string
		streamName    string
		expectedError error
	}{
		{"ReadEventsForwardAsyncOk", "my-stream", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := MakeDriver(t)
			st, err := store.NewPersistentStore(store.WithDriver(d))
			assert.NoError(t, err)

			name := fmt.Sprintf("events_%s", tt.streamName)

			d.Streams[name] = append(d.Streams[name], &pbMessaging.Event{Id: "1"}, &pbMessaging.Event{Id: "2"}, &pbMessaging.Event{Id: "3"})

			eventsCh, err := st.ReadEventsForwardAsync(tt.streamName, 0, 3)

			assert.Equal(t, tt.expectedError, err)

			i := 0
			for e := range eventsCh {
				i++
				assert.Equal(t, fmt.Sprintf("%v", i), e.GetId())
			}
		})
	}
}
