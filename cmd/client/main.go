package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/happendb/happendb/internal/client"
	"github.com/happendb/happendb/pkg/messaging"
	pb "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	client, err := client.NewStoreClient()

	if err != nil {
		return err
	}

	_, err = client.ReadEvents(context.Background(), &pb.ReadEventsRequest{
		AggregateId: args[0],
	})

	if err != nil {
		return err
	}

	stream := messaging.NewEventStream("foo")

	_, err = client.Append(context.Background(), &pb.AppendRequest{
		Stream: stream.EventStream,
		Events: []*messaging.Event{},
	})

	if err != nil {
		return err
	}

	return nil
}
