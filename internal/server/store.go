package server

import (
	"context"
	"net"

	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/postgres"
	messaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	pb "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// StoreServer ...
type StoreServer struct {
	grpcServer     *grpc.Server
	readOnlyStore  store.ReadOnlyStore
	writeOnlyStore store.WriteOnlyStore
}

// NewStoreServer ...
func NewStoreServer() (*StoreServer, error) {
	store, err := postgres.NewPostgresStore(postgres.WithPersistMode(store.PersistModeSingleTable))

	if err != nil {
		return nil, err
	}

	srv := &StoreServer{
		grpc.NewServer(),
		store,
		store,
	}

	pb.RegisterReadOnlyServiceServer(srv.grpcServer, srv)
	pb.RegisterWriteOnlyServiceServer(srv.grpcServer, srv)

	return srv, nil
}

// Run ...
func (s StoreServer) Run() error {
	lis, err := net.Listen("tcp", ":3000")

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

// ReadEvents ...
func (s StoreServer) ReadEvents(ctx context.Context, req *pb.ReadEventsRequest) (*pb.ReadEventsResponse, error) {
	var (
		err    error
		stream *messaging.EventStream
		events []*messaging.Event
	)

	if stream, err = s.readOnlyStore.ReadEvents(req.GetAggregateId()); err != nil {
		return nil, err
	}

	for _, event := range stream.GetEvents() {
		events = append(events, event)
	}

	log.WithFields(log.Fields{
		"stream_name":   stream.GetName(),
		"stream_length": len(stream.GetEvents()),
	}).Debugf("%T::ReadEvents(%#v)\n", s, req)

	return &pb.ReadEventsResponse{
		AggregateId: req.GetAggregateId(),
		Events:      events,
	}, nil
}

// Append ...
func (s StoreServer) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	log.WithFields(log.Fields{
		"stream_name": req.GetStream().GetName,
	}).Debugf("%T::Append(%#v)\n", s, req.GetEvents())

	err := s.writeOnlyStore.Append(req.GetStream().GetName(), req.GetEvents()...)

	if err != nil {
		return nil, err
	}

	return &pb.AppendResponse{}, nil
}
