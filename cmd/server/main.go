package main

import (
	"io"
	"os"

	"github.com/happendb/happendb/internal/server"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)

	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	srv, err := server.NewReadOnlyServer()

	if err != nil {
		return err
	}

	return srv.Run()
}
