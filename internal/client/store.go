package client

import (
	"context"

	pb "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// StoreClient ...
type StoreClient struct {
	conn            grpc.ClientConnInterface
	readOnlyClient  pb.ReadOnlyServiceClient
	writeOnlyClient pb.WriteOnlyServiceClient
}

// NewStoreClient ...
func NewStoreClient() (*StoreClient, error) {
	conn, err := grpc.Dial("localhost:3000", grpc.WithInsecure())

	if err != nil {
		return nil, err
	}

	return &StoreClient{
		conn,
		pb.NewReadOnlyServiceClient(conn),
		pb.NewWriteOnlyServiceClient(conn),
	}, nil
}

// ReadEvents ...
func (c *StoreClient) ReadEvents(ctx context.Context, req *pb.ReadEventsRequest, opts ...grpc.CallOption) (*pb.ReadEventsResponse, error) {
	response, err := c.readOnlyClient.ReadEvents(ctx, req, opts...)

	if err != nil {
		return response, err
	}

	log.Info(response)

	return response, err
}

// Append ...
func (c *StoreClient) Append(ctx context.Context, req *pb.AppendRequest, opts ...grpc.CallOption) (*pb.AppendResponse, error) {
	response, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return response, err
	}

	log.Info(response)

	return response, err
}
