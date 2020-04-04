package driver_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/uuid"
	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/driver"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/stretchr/testify/assert"
)

const (
	loginsJSON string = `
[
	{
		id": 1,
		email": "jdymond0@1688.com",
		ip_address": "17.87.188.161"
	}, {
		id": 2,
		email": "jduplan1@ebay.co.uk",
		ip_address": "58.12.80.50"
	}, {
		id": 3,
		email": "ewigelsworth2@bloomberg.com",
		ip_address": "142.128.8.144"
	}
]
`
)

func MakeEvent(t *testing.T, version uint64) *pbMessaging.Event {
	uuid, err := uuid.NewRandom()
	assert.NoError(t, err)

	return &pbMessaging.Event{
		Id: uuid.String(),
		Metadata: &any.Any{
			Value: []byte{},
		},
		Payload: &any.Any{
			Value: []byte(loginsJSON),
		},
		Time:    time.Now().Format(time.RFC3339Nano),
		Type:    "users.logged_in",
		Version: version,
	}
}

func TestPostgresNewDriver(t *testing.T) {
	driver, err := driver.NewPostgresDriver("sslmode=disable host=localhost port=5432 dbname=happendb_test user=postgres password=123", store.PersistModeSingleTable)

	assert.NotNil(t, driver, "expected not nil driver")
	assert.NoError(t, err)
}

func TestPostgresNewDriverError(t *testing.T) {
	driver, err := driver.NewPostgresDriver("ms:'qwe/12[p3klp[bad dsn", store.PersistModeSingleTable)

	assert.Nil(t, driver, "expected nil driver")
	assert.NotNil(t, err, "expected not nil error")
}

func TestPostgresCreateStream(t *testing.T) {
	tests := []struct {
		name           string
		streamName     string
		expectedStream *store.Stream
		expectedError  error
	}{
		{"CreateStreamOK", "my_stream", &store.Stream{Name: "my_stream"}, nil},
		{"CreateStreamError", "", nil, errors.New("invalid stream name ''")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := driver.NewPostgresDriver("sslmode=disable host=localhost port=5432 dbname=happendb_test user=postgres password=123", store.PersistModeSingleTable)

			assert.NotNil(t, driver, "%s: expected not nil driver", tt.name)
			assert.NoError(t, err)

			var streamName string

			if tt.expectedStream != nil {
				assert.NoError(t, driver.DeleteStream(tt.expectedStream.Name))
				streamName = tt.expectedStream.Name
			}

			stream, err := driver.CreateStream(streamName)

			assert.Equal(t, tt.expectedStream, stream)
			assert.Equal(t, tt.expectedError, err)
		})
	}
}

func TestPostgresAppend(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"AppendOK"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := driver.NewPostgresDriver("sslmode=disable host=localhost port=5432 dbname=happendb_test user=postgres password=123", store.PersistModeSingleTable)

			assert.NotNil(t, driver, "%s: expected not nil driver", tt.name)
			assert.NoError(t, err)

			uuid, err := uuid.NewRandom()

			assert.NoError(t, err)
			aggregateID := uuid.String()

			driver.Append(
				aggregateID,
				1,
				MakeEvent(t, 1),
				MakeEvent(t, 2),
				MakeEvent(t, 3))
		})
	}
}
