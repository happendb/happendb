package main

import (
	"io"
	"log"
	"os"

	"github.com/happendb/happendb/internal/server"
)

func main() {
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
