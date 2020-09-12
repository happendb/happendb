package main

//import (
//	"context"
//	"flag"
//	"time"
//
//	"github.com/golang/protobuf/ptypes/any"
//	"github.com/google/uuid"
//	"github.com/rs/zerolog/log"
//
//	"github.com/happendb/happendb/internal/client"
//	"github.com/happendb/happendb/internal/config"
//	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
//	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
//)
//
//var (
//	cli *client.StoreClient
//)
//
//func init() {
//	config.InitLogging()
//
//	var err error
//	cli, err = client.NewStoreClient()
//
//	if err != nil {
//		fail("could not create store cl", err)
//	}
//}
//
//func main() {
//	var (
//		err   error
//		ctx          = context.Background()
//		args         = flag.Args()
//		count uint64 = 100
//	)
//
//	t := time.Now()
//	var newEvents []*pbMessaging.Event
//
//	for i := 1; i <= 3; i++ {
//		id, err := uuid.NewRandom()
//
//		if err != nil {
//			fail("failed to generate id", err)
//		}
//
//		newEvents = append(newEvents, &pbMessaging.Event{
//			Id:      id.String(),
//			Time:    t.Format(time.RFC3339),
//			Version: 0,
//			Metadata: &any.Any{
//				Value: []byte("{}"),
//			},
//			Payload: &any.Any{
//				Value: []byte(`{
//  "id": 54,
//  "first_name": "Orton",
//  "last_name": "Gotcher",
//  "email": "ogotcher1h@mozilla.com",
//  "ip_address": "247.175.206.166"
//}`),
//			},
//			Type: "user.logged_in",
//		})
//	}
//
//	_, err = cli.Append(ctx, &pbStore.AppendRequest{
//		StreamName:      args[0],
//		ExpectedVersion: 3,
//		Events:          newEvents,
//	})
//
//	if err != nil {
//		fail("could not append events", err)
//	}
//
//	res, err := cli.ReadEventsForward(ctx, &pbStore.SyncReadEventsForwardRequest{
//		Stream: args[0],
//		Start:  0,
//		Count:  count,
//	})
//
//	if err != nil {
//		fail("could not read stream events forward", err)
//	}
//
//	cli, err := cli.ReadEventsForwardAsync(ctx, &pbStore.AsyncReadEventsForwardRequest{
//		Stream: args[0],
//		Start:  0,
//		Count:  count,
//	})
//
//	if err != nil {
//		fail("could not read stream events forward async", err)
//	}
//
//	_ = cli
//
//	_ = res
//}
//
//func fail(msg string, err error) {
//	log.Fatal().Err(err).Msg(msg)
//}
