package client

import (
	"context"

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
func NewStoreClient() (*StoreClient, error) {
	conn, err := grpc.Dial("localhost:3000", grpc.WithInsecure())

	if err != nil {
		return nil, err
	}

	return &StoreClient{
		conn,
		pbStore.NewReadOnlyServiceClient(conn),
		pbStore.NewWriteOnlyServiceClient(conn),
	}, nil
}

// ReadEvents ...
func (c *StoreClient) ReadEvents(ctx context.Context, req *pbStore.ReadEventsRequest, opts ...grpc.CallOption) (*pbStore.ReadEventsResponse, error) {
	response, err := c.readOnlyClient.ReadEvents(ctx, req, opts...)

	if err != nil {
		return response, err
	}

	log.Infof("%#v\n", response)

	return response, err
}

// Append ...
func (c *StoreClient) Append(ctx context.Context, req *pbStore.AppendRequest, opts ...grpc.CallOption) (*pbStore.AppendResponse, error) {
	response, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return response, err
	}

	log.Infof("%#v\n", response)

	return response, err
}
