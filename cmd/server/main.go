package main

import (
	"os"
	"time"

	"github.com/happendb/happendb/internal/server"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339Nano,
	})

	srv, err := server.NewStoreServer()

	if err != nil {
		log.Fatal().Err(err).Msg("could not create store server")
	}

	if err := srv.Run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		log.Fatal().Err(err).Msg("failed to run")
	}
}
