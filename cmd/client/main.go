package main

import (
	"context"
	"os"

	"github.com/happendb/happendb/internal/client"
	pb "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	var (
		args         = os.Args[1:]
		start uint64 = 0
		count uint64 = 100
	)

	client, err := client.NewStoreClient()

	if err != nil {
		log.Fatal().Err(err).Msg("could not create store client")
	}

	_, err = client.ReadStreamEventsForwardAsync(context.Background(), &pb.AsyncReadStreamEventsForwardRequest{
		Stream: args[0],
		Start:  start,
		Count:  count,
	})

	if err != nil {
		log.Fatal().Err(err).Msg("could not read stream events forward async")
	}

	_, err = client.ReadStreamEventsForward(context.Background(), &pb.SyncReadStreamEventsForwardRequest{
		Stream: args[0],
		Start:  start,
		Count:  count,
	})
}
