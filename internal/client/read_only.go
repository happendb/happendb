package client

import (
	"context"

	pb "github.com/happendb/happendb/proto/gen/go/store"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// ReadOnlyClient ...
type ReadOnlyClient struct {
	conn   grpc.ClientConnInterface
	client pb.ReadOnlyServiceClient
}

// NewReadOnlyClient ...
func NewReadOnlyClient() (*ReadOnlyClient, error) {
	conn, err := grpc.Dial("localhost:1232", grpc.WithInsecure())

	if err != nil {
		return nil, err
	}

	return &ReadOnlyClient{
		conn,
		pb.NewReadOnlyServiceClient(conn),
	}, nil
}

// ReadEvents ...
func (c *ReadOnlyClient) ReadEvents(ctx context.Context, req *pb.ReadEventsRequest, opts ...grpc.CallOption) (*pb.ReadEventsResponse, error) {
	response, err := c.client.ReadEvents(ctx, req, opts...)

	if err != nil {
		return response, err
	}

	log.Info(response)

	return response, err
}
