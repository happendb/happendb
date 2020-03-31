package main

import (
	"context"
	"os"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/uuid"
	"github.com/happendb/happendb/internal/client"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339Nano,
	})

	ctx := context.Background()

	var (
		args         = os.Args[1:]
		start uint64 = 0
		count uint64 = 100
	)

	client, err := client.NewStoreClient()

	if err != nil {
		fail("could not create store client", err)
	}

	t := time.Now()
	events := []*pbMessaging.Event{}

	for i := 0; i < 3; i++ {
		uuid, err := uuid.NewRandom()

		if err != nil {
			fail("failed to generate uuid", err)
		}

		events = append(events, &pbMessaging.Event{
			Id:      uuid.String(),
			Time:    t.Format(time.RFC3339),
			Version: uint64(4 + i),
			Metadata: &any.Any{
				Value: []byte("{}"),
			},
			Payload: &any.Any{
				Value: []byte(`{
  "id": 54,
  "first_name": "Orton",
  "last_name": "Gotcher",
  "email": "ogotcher1h@mozilla.com",
  "ip_address": "247.175.206.166"
}`),
			},
			Type: "user.logged_in",
		})
	}

	_, err = client.Append(ctx, &pbStore.AppendRequest{
		StreamName:      args[0],
		ExpectedVersion: 4,
		Events:          events,
	})

	if err != nil {
		fail("could not append events", err)
	}

	_, err = client.ReadStreamEventsForwardAsync(ctx, &pbStore.AsyncReadStreamEventsForwardRequest{
		Stream: args[0],
		Start:  start,
		Count:  count,
	})

	if err != nil {
		fail("could not read stream events forward async", err)
	}

	_, err = client.ReadStreamEventsForward(ctx, &pbStore.SyncReadStreamEventsForwardRequest{
		Stream: args[0],
		Start:  start,
		Count:  count,
	})
}

func fail(msg string, err error) {
	log.Fatal().Err(err).Msg(msg)
}
