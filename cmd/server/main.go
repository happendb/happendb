package main

import (
	"context"
	"os"

	"github.com/happendb/happendb/internal/config"
	"github.com/happendb/happendb/internal/server"
	"github.com/rs/zerolog/log"
)

func main() {
	config.InitLogging()

	ctx := context.Background()
	srv, err := server.NewStoreServer("sslmode=disable host=localhost port=5432 dbname=happendb user=postgres password=123")

	if err != nil {
		fail("could not create store server", err)
	}

	if err := srv.Run(ctx, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fail("failed to run", err)
	}
}

func fail(msg string, err error) {
	log.Fatal().Err(err).Msg(msg)
}
