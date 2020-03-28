package main

import (
	"context"
	"io"
	"os"

	"github.com/happendb/happendb/internal/client"
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
	aggregateID := args[0]
	client, err := client.NewStoreClient()

	if err != nil {
		return err
	}

	_, err = client.ReadEvents(context.Background(), &pb.ReadEventsRequest{
		AggregateId: aggregateID,
	})

	if err != nil {
		return err
	}

	// uuid, err := uuid.NewRandom()

	// if err != nil {
	// 	return err
	// }

	// now := time.Now()

	// _, err = client.Append(context.Background(), &pb.AppendRequest{
	// 	StreamName: aggregateID,
	// 	Events: []*v1.Event{
	// 		{
	// 			Id:   uuid.String(),
	// 			Type: "event.LoggedIn",
	// 			Time: now.Format("2006-01-02 15:04:05"),
	// 			Payload: &any.Any{
	// 				Value: []byte(`
	// {
	//   "id": 92,
	//   "first_name": "Gabey",
	//   "last_name": "Kimbley",
	//   "email": "gkimbley2j@businessweek.com",
	//   "gender": "Female",
	//   "ip_address": "13.242.243.177"
	// }
	// `),
	// 			},
	// 		},
	// 	},
	// })

	// if err != nil {
	// 	return err
	// }

	return nil
}
