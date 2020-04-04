package client

import (
	"context"
	"io"

	"github.com/happendb/happendb/internal/logtime"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

// StoreClient ...
type StoreClient struct {
	conn            grpc.ClientConnInterface
	syncReader      pbStore.SyncReaderServiceClient
	asyncReader     pbStore.AsyncReaderServiceClient
	writeOnlyClient pbStore.WriteOnlyServiceClient
}

// NewStoreClient ...
func NewStoreClient() (cli *StoreClient, err error) {
	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())

	if err != nil {
		return
	}

	cli = &StoreClient{
		conn:            conn,
		syncReader:      pbStore.NewSyncReaderServiceClient(conn),
		asyncReader:     pbStore.NewAsyncReaderServiceClient(conn),
		writeOnlyClient: pbStore.NewWriteOnlyServiceClient(conn),
	}

	log.Debug().Msg("client connected")

	return
}

// ReadEventsForward ...
func (c *StoreClient) ReadEventsForward(ctx context.Context, req *pbStore.SyncReadEventsForwardRequest, opts ...grpc.CallOption) (*pbStore.SyncReadEventsForwardResponse, error) {
	defer logtime.Elapsedf("ReadEventsForward")()

	res, err := c.syncReader.ReadEventsForward(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ReadEventsForwardAsync ...
func (c *StoreClient) ReadEventsForwardAsync(ctx context.Context, req *pbStore.AsyncReadEventsForwardRequest, opts ...grpc.CallOption) (pbStore.AsyncReaderService_ReadEventsForwardAsyncClient, error) {
	defer logtime.Elapsedf("ReadEventsForwardAsync")()

	stream, err := c.asyncReader.ReadEventsForwardAsync(ctx, req, opts...)

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

		log.Debug().Interface("event_id", event.GetId()).Msg("event received")
	}
}

// Append ...
func (c *StoreClient) Append(ctx context.Context, req *pbStore.AppendRequest, opts ...grpc.CallOption) (*pbStore.AppendResponse, error) {
	defer logtime.Elapsedf("Append")()

	res, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	return res, err
}
