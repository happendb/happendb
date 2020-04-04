package server

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/driver"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

const (
	port = 9000
)

// StoreServer ...
type StoreServer struct {
	grpcServer     *grpc.Server
	readOnlyStore  store.ReaderStore
	writeOnlyStore store.WriteOnlyStore
}

// NewStoreServer ...
func NewStoreServer(dsn string) (srv *StoreServer, err error) {
	driver, err := driver.NewPostgresDriver(dsn, store.PersistModeSingleTable)

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
		grpcServer:     grpc.NewServer(),
		readOnlyStore:  persistentStore,
		writeOnlyStore: persistentStore,
	}

	pbStore.RegisterSyncReaderServiceServer(srv.grpcServer, srv)
	pbStore.RegisterAsyncReaderServiceServer(srv.grpcServer, srv)
	pbStore.RegisterWriteOnlyServiceServer(srv.grpcServer, srv)

	return
}

// Run ...
func (s *StoreServer) Run(_ context.Context, args []string, stdin io.Reader, stdout io.Writer) error {
	log.Info().Msgf("listening on port %v", port)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

// ReadEventsForward ...
func (s *StoreServer) ReadEventsForward(ctx context.Context, req *pbStore.SyncReadEventsForwardRequest) (*pbStore.SyncReadEventsForwardResponse, error) {
	events, err := s.readOnlyStore.ReadEventsForward(req.GetStream(), req.GetStart(), req.GetCount())

	if err != nil {
		return nil, err
	}

	return &pbStore.SyncReadEventsForwardResponse{
		Events: events,
	}, err
}

// ReadEventsForwardAsync ...
func (s *StoreServer) ReadEventsForwardAsync(req *pbStore.AsyncReadEventsForwardRequest, stream pbStore.AsyncReaderService_ReadEventsForwardAsyncServer) error {
	eventsChannel, err := s.readOnlyStore.ReadEventsForwardAsync(req.GetStream(), req.GetStart(), req.GetCount())

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
	err := s.writeOnlyStore.Append(req.GetStreamName(), req.GetExpectedVersion(), req.GetEvents()...)

	if err != nil {
		return nil, err
	}

	return &pbStore.AppendResponse{}, nil
}
