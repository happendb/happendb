package driver_test

import (
	"errors"
	"testing"

	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/driver"
	"github.com/stretchr/testify/assert"
)

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
		{"CreateStream", "my_stream", &store.Stream{Name: "my_stream"}, nil},
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
