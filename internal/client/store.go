package client

import (
	"context"
	"io"

	"github.com/happendb/happendb/internal/debug"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	log "github.com/sirupsen/logrus"
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

	return
}

// ReadStreamEventsForward ...
func (c *StoreClient) ReadStreamEventsForward(ctx context.Context, req *pbStore.SyncReadStreamEventsForwardRequest, opts ...grpc.CallOption) (*pbStore.SyncReadStreamEventsForwardResponse, error) {
	defer log.Debug(debug.Elapsedf("[%T::ReadStreamEventsForward]", c)())

	res, err := c.syncReader.ReadStreamEventsForward(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	return res, nil
}

// ReadStreamEventsForwardAsync ...
func (c *StoreClient) ReadStreamEventsForwardAsync(ctx context.Context, req *pbStore.AsyncReadStreamEventsForwardRequest, opts ...grpc.CallOption) (pbStore.AsyncReaderService_ReadStreamEventsForwardAsyncClient, error) {
	defer log.Debug(debug.Elapsedf("[%T::ReadStreamEventsForwardAsync]", c)())

	stream, err := c.asyncReader.ReadStreamEventsForwardAsync(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	for {
		var e *pbMessaging.Event

		if e, err = stream.Recv(); err == io.EOF {
			return stream, nil
		}

		if err != nil {
			return nil, err
		}

		log.WithField("id", e.GetId()).Debugf("[%T::ReadStreamEventsForwardAsync] event received", c)
	}
}

// Append ...
func (c *StoreClient) Append(ctx context.Context, req *pbStore.AppendRequest, opts ...grpc.CallOption) (*pbStore.AppendResponse, error) {
	defer log.WithField("request", req).Debugf("[%T::Append] %v", c, debug.Elapsedf("%T::Append", c)())

	res, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	return res, err
}
