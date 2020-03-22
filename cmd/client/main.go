package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/happendb/happendb/internal/client"
	pb "github.com/happendb/happendb/proto/gen/go/store"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	client, err := client.NewReadOnlyClient()

	if err != nil {
		return err
	}

	client.ReadEvents(context.Background(), &pb.ReadEventsRequest{
		AggregateID: args[0],
	})

	return nil
}
