package main

import (
	"context"
	"flag"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/uuid"
	"github.com/happendb/happendb/internal/client"
	"github.com/happendb/happendb/internal/config"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	"github.com/rs/zerolog/log"
)

func main() {
	config.InitLogging()

	var (
		ctx          = context.Background()
		args         = flag.Args()
		start uint64 = 0
		count uint64 = 100
	)

	client, err := client.NewStoreClient()

	if err != nil {
		fail("could not create store client", err)
	}

	cli, err := client.ReadEventsForwardAsync(ctx, &pbStore.AsyncReadEventsForwardRequest{
		Stream: args[0],
		Start:  start,
		Count:  count,
	})

	if err != nil {
		fail("could not read stream events forward async", err)
	}

	_ = cli

	res, err := client.ReadEventsForward(ctx, &pbStore.SyncReadEventsForwardRequest{
		Stream: args[0],
		Start:  start,
		Count:  count,
	})

	currentVersion := uint64(0)
	events := res.GetEvents()

	if len(events) > 0 {
		currentVersion = events[len(events)-1].GetVersion()
	}

	t := time.Now()
	newEvents := []*pbMessaging.Event{}

	for i := 1; i <= 3; i++ {
		uuid, err := uuid.NewRandom()

		if err != nil {
			fail("failed to generate uuid", err)
		}

		newEvents = append(newEvents, &pbMessaging.Event{
			Id:      uuid.String(),
			Time:    t.Format(time.RFC3339),
			Version: currentVersion,
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
		ExpectedVersion: currentVersion,
		Events:          newEvents,
	})

	if err != nil {
		fail("could not append events", err)
	}
}

func fail(msg string, err error) {
	log.Fatal().Err(err).Msg(msg)
}
