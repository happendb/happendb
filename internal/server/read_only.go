package server

import (
	"context"
	"net"

	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/postgres"
	pb "github.com/happendb/happendb/proto/gen/go/store"
	"google.golang.org/grpc"
)

// ReadOnlyServer ...
type ReadOnlyServer struct {
	grpcServer *grpc.Server
	store      store.ReadOnlyStore
}

// NewReadOnlyServer ...
func NewReadOnlyServer() (*ReadOnlyServer, error) {
	store, err := postgres.NewPostgresStore()

	if err != nil {
		return nil, err
	}

	srv := &ReadOnlyServer{
		grpc.NewServer(),
		store,
	}

	pb.RegisterReadOnlyServiceServer(srv.grpcServer, srv)

	return srv, nil
}

// Run ...
func (s *ReadOnlyServer) Run() error {
	lis, err := net.Listen("tcp", ":1232")

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

// ReadEvents ...
func (s *ReadOnlyServer) ReadEvents(ctx context.Context, req *pb.ReadEventsRequest) (*pb.ReadEventsResponse, error) {
	events, err := s.store.ReadEvents(req.GetAggregateID())

	if err != nil {
		return nil, err
	}

	return &pb.ReadEventsResponse{
		AggregateID: req.GetAggregateID(),
		Events:      events,
	}, nil
}
