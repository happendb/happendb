package server

import (
	"context"
	"net"

	"github.com/happendb/happendb/pkg/messaging"
	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/postgres"
	pb "github.com/happendb/happendb/proto/gen/go/store"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// ReadOnlyServer ...
type ReadOnlyServer struct {
	grpcServer *grpc.Server
	store      store.ReadOnlyStore
}

// NewReadOnlyServer ...
func NewReadOnlyServer() (*ReadOnlyServer, error) {
	store, err := postgres.NewPostgresStore(postgres.WithPersistMode(store.PersistModeSingleTable))

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
func (s ReadOnlyServer) Run() error {
	lis, err := net.Listen("tcp", ":3000")

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

// ReadEvents ...
func (s ReadOnlyServer) ReadEvents(ctx context.Context, req *pb.ReadEventsRequest) (*pb.ReadEventsResponse, error) {
	var (
		err    error
		stream *messaging.EventStream
		events []*messaging.Event
	)

	if stream, err = s.store.ReadEvents(req.GetAggregateID()); err != nil {
		return nil, err
	}

	for event := range stream.Events() {
		events = append(events, event)
	}

	log.WithFields(log.Fields{
		"stream_name":   stream.Name(),
		"stream_length": stream.Len(),
	}).Debugf("%T::ReadEvents(%#v)\n", s, req)

	return &pb.ReadEventsResponse{
		AggregateId: req.GetAggregateID(),
		Events:      events,
	}, nil
}
