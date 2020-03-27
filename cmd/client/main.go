package main

import (
	"context"
	"io"
	"os"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/uuid"
	"github.com/happendb/happendb/internal/client"
	"github.com/happendb/happendb/pkg/messaging"
	v1 "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pb "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)

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

	stream := &messaging.EventStream{
		EventStream: &v1.EventStream{
			Name: "foo",
		},
	}

	uuid, err := uuid.NewRandom()

	if err != nil {
		return err
	}

	_, err = client.Append(context.Background(), &pb.AppendRequest{
		Stream: stream.EventStream,
		Events: []*v1.Event{
			{
				Id:          uuid.String(),
				Type:        "foo",
				AggregateId: uuid.String(),
				Payload: &any.Any{
					Value: []byte(`
	{
	  "id": 92,
	  "first_name": "Gabey",
	  "last_name": "Kimbley",
	  "email": "gkimbley2j@businessweek.com",
	  "gender": "Female",
	  "ip_address": "13.242.243.177"
	}
	`),
				},
			},
		},
	})

	if err != nil {
		return err
	}

	return nil
}
