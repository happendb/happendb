package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/happendb/happendb/logtime"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
	"github.com/happendb/happendb/store"
	"github.com/happendb/happendb/store/driver"
)

const (
	port = 9000
)

type StoreServer struct {
	grpcServer  *grpc.Server
	storeReader store.Reader
	storeWriter store.Writer
}

func NewPersistentStoreServer() (*StoreServer, error) {
	drv, err := driver.NewMemoryDriver()

	if err != nil {
		return nil, err
	}

	persistentStore, err := store.NewPersistentStore(store.WithDriver(drv))

	if err != nil {
		return nil, err
	}

	srv := &StoreServer{
		grpcServer:  grpc.NewServer(),
		storeReader: persistentStore,
		storeWriter: persistentStore,
	}

	pbStore.RegisterSyncReaderServiceServer(srv.grpcServer, srv)
	pbStore.RegisterAsyncReaderServiceServer(srv.grpcServer, srv)
	pbStore.RegisterWriterServiceServer(srv.grpcServer, srv)

	reflection.Register(srv.grpcServer)

	return srv, nil
}

func (s *StoreServer) Run(_ context.Context, args []string, stdin io.Reader, stdout io.Writer) error {
	log.Info().Msgf("listening on port %v", port)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

func (s *StoreServer) Append(_ context.Context, req *pbStore.AppendRequest) (*pbStore.AppendResponse, error) {
	defer logtime.Elapsedf("Append")()

	stream, err := s.storeWriter.Append(req.GetStreamName(), req.GetExpectedVersion(), req.GetEvents()...)

	if err != nil {
		return nil, err
	}

	return &pbStore.AppendResponse{StreamVersion: stream.CurrentVersion}, nil
}

func (s *StoreServer) ReadEventsForward(ctx context.Context, req *pbStore.SyncReadEventsForwardRequest) (*pbStore.SyncReadEventsForwardResponse, error) {
	defer logtime.Elapsedf("ReadEventsForward")()

	events, err := s.storeReader.ReadEventsForward(req.GetStream(), req.GetStart(), req.GetCount())

	if err != nil {
		return nil, err
	}

	return &pbStore.SyncReadEventsForwardResponse{
		Events: events,
	}, err
}

func (s *StoreServer) ReadEventsForwardAsync(req *pbStore.AsyncReadEventsForwardRequest, stream pbStore.AsyncReaderService_ReadEventsForwardAsyncServer) error {
	defer logtime.Elapsedf("ReadEventsForwardAsync")()

	for {
		eventsChannel, err := s.storeReader.ReadEventsForwardAsync(req.GetStream(), req.GetStart(), req.GetCount())

		if err != nil {
			return err
		}

		if eventsChannel != nil && len(eventsChannel) > 0 {
			for event := range eventsChannel {
				if err := stream.Send(event); err != nil {
					return err
				}

				time.Sleep(1 * time.Millisecond)
			}
		}

		log.Debug().Msgf("received (%v) events", len(eventsChannel))
		time.Sleep(25 * time.Millisecond)
	}
}
