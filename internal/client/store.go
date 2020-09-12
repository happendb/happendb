package client

import (
	"context"
	"io"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	"github.com/happendb/happendb/logtime"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
)

type StoreClient struct {
	conn            grpc.ClientConnInterface
	syncReader      pbStore.SyncReaderServiceClient
	asyncReader     pbStore.AsyncReaderServiceClient
	writeOnlyClient pbStore.WriterServiceClient
}

func NewStoreClient() (cli *StoreClient, err error) {
	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())

	if err != nil {
		return
	}

	cli = &StoreClient{
		conn:            conn,
		syncReader:      pbStore.NewSyncReaderServiceClient(conn),
		asyncReader:     pbStore.NewAsyncReaderServiceClient(conn),
		writeOnlyClient: pbStore.NewWriterServiceClient(conn),
	}

	log.Debug().Msg("client connected")

	return
}

func (c *StoreClient) Append(ctx context.Context, req *pbStore.AppendRequest, opts ...grpc.CallOption) (*pbStore.AppendResponse, error) {
	defer logtime.Elapsedf("Append")()

	res, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	return res, err
}

func (c *StoreClient) ReadEventsForward(ctx context.Context, req *pbStore.SyncReadEventsForwardRequest, opts ...grpc.CallOption) (*pbStore.SyncReadEventsForwardResponse, error) {
	defer logtime.Elapsedf("ReadEventsForward")()

	res, err := c.syncReader.ReadEventsForward(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	log.Info().Interface("events", res.GetEvents()).Msg("events received")

	return res, nil
}

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

		log.Info().Interface("event", event).Msg("event received")
	}
}
