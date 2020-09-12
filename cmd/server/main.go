package main

import (
	"context"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/happendb/happendb/internal/config"
	"github.com/happendb/happendb/internal/server"
)

var (
	srv *server.StoreServer
)

func init() {
	config.InitLogging()

	var err error
	srv, err = server.NewPersistentStoreServer( /*"sslmode=disable host=localhost port=5432 dbname=happendb user=postgres password=123"*/ )

	if err != nil {
		fail("could not create store server", err)
	}
}

func main() {
	if err := srv.Run(context.Background(), os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fail("failed to run", err)
	}
}

func fail(msg string, err error) {
	log.Fatal().Err(err).Msg(msg)
}
