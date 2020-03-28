package client

import (
	"context"
	"io"
	"time"

	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// StoreClient ...
type StoreClient struct {
	conn            grpc.ClientConnInterface
	readOnlyClient  pbStore.ReadOnlyServiceClient
	writeOnlyClient pbStore.WriteOnlyServiceClient
}

// NewStoreClient ...
func NewStoreClient() (cli *StoreClient, err error) {
	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())

	if err != nil {
		return
	}

	cli = &StoreClient{
		conn,
		pbStore.NewReadOnlyServiceClient(conn),
		pbStore.NewWriteOnlyServiceClient(conn),
	}

	return
}

// ReadEvents ...
func (c StoreClient) ReadEvents(ctx context.Context, req *pbStore.ReadEventsRequest, opts ...grpc.CallOption) (pbStore.ReadOnlyService_ReadEventsClient, error) {
	stream, err := c.readOnlyClient.ReadEvents(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	for {
		var event *pbMessaging.Event

		if event, err = stream.Recv(); err == io.EOF {
			return stream, nil
		}

		if err != nil {
			return nil, err
		}

		log.WithField("event", event).Debugf("[%T::ReadEvents] event received\n", c)
		time.Sleep(time.Millisecond * 200)
	}
}

// Append ...
func (c StoreClient) Append(ctx context.Context, req *pbStore.AppendRequest, opts ...grpc.CallOption) (*pbStore.AppendResponse, error) {
	res, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	log.WithField("request", req).Debugf("%T::Append\n", c)

	return res, err
}
