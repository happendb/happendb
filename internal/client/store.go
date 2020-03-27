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
func NewStoreClient() (cli *StoreClient, err error) {
	conn, err := grpc.Dial("localhost:3000", grpc.WithInsecure())

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
func (c *StoreClient) ReadEvents(ctx context.Context, req *pbStore.ReadEventsRequest, opts ...grpc.CallOption) (res *pbStore.ReadEventsResponse, err error) {
	res, err = c.readOnlyClient.ReadEvents(ctx, req, opts...)

	log.WithFields(log.Fields{"req": req}).Debugf("%T::ReadEvents\n", c)

	return
}

// Append ...
func (c *StoreClient) Append(ctx context.Context, req *pbStore.AppendRequest, opts ...grpc.CallOption) (*pbStore.AppendResponse, error) {
	res, err := c.writeOnlyClient.Append(ctx, req, opts...)

	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{"res": res}).Debugf("%T::Append\n", c)

	return res, err
}
