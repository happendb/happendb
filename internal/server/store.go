package server

import (
	"context"
	"net"

	"github.com/happendb/happendb/internal/debug"
	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/driver"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// StoreServer ...
type StoreServer struct {
	grpcServer     *grpc.Server
	readOnlyStore  store.ReaderStore
	writeOnlyStore store.WriteOnlyStore
}

// NewStoreServer ...
func NewStoreServer() (srv *StoreServer, err error) {
	driver, err := driver.NewPostgresDriver()

	if err != nil {
		return
	}

	persistentStore, err := store.NewPersistentStore(
		store.WithDriver(driver),
		store.WithPersistMode(store.PersistModeSingleTable),
	)

	if err != nil {
		return
	}

	srv = &StoreServer{
		grpc.NewServer(),
		persistentStore,
		persistentStore,
	}

	pbStore.RegisterSyncReaderServiceServer(srv.grpcServer, srv)
	pbStore.RegisterAsyncReaderServiceServer(srv.grpcServer, srv)
	pbStore.RegisterWriteOnlyServiceServer(srv.grpcServer, srv)

	return
}

// Run ...
func (s StoreServer) Run() error {
	lis, err := net.Listen("tcp", ":9000")

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

// ReadStreamEventsForward ...
func (s *StoreServer) ReadStreamEventsForward(ctx context.Context, req *pbStore.SyncReadStreamEventsForwardRequest) (*pbStore.SyncReadStreamEventsForwardResponse, error) {
	defer log.WithField("request", req).Debug(debug.Elapsedf("[%T::ReadStreamEventsForward]", s)())

	events, err := s.readOnlyStore.ReadStreamEventsForward(req.GetStream(), req.GetStart(), req.GetCount())

	if err != nil {
		return nil, err
	}

	return &pbStore.SyncReadStreamEventsForwardResponse{
		Events: events,
	}, err
}

// ReadStreamEventsForwardAsync ...
func (s *StoreServer) ReadStreamEventsForwardAsync(req *pbStore.AsyncReadStreamEventsForwardRequest, stream pbStore.AsyncReaderService_ReadStreamEventsForwardAsyncServer) error {
	defer log.WithField("request", req).Debug(debug.Elapsedf("[%T::ReadStreamEventsForwardAsync]", s)())

	eventsChannel, err := s.readOnlyStore.ReadStreamEventsForwardAsync(req.GetStream(), req.GetStart(), req.GetCount())

	if err != nil {
		return err
	}

	for event := range eventsChannel {
		stream.Send(event)
	}

	return nil
}

// Append ...
func (s *StoreServer) Append(ctx context.Context, req *pbStore.AppendRequest) (*pbStore.AppendResponse, error) {
	defer log.WithField("request", req).Debug(debug.Elapsedf("[%T::Append]", s)())

	err := s.writeOnlyStore.Append(req.GetStreamName(), req.GetEvents()...)

	if err != nil {
		return nil, err
	}

	return &pbStore.AppendResponse{}, nil
}
