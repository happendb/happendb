package server

import (
	"context"
	"net"

	"github.com/happendb/happendb/pkg/messaging"
	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/driver"
	pbStore "github.com/happendb/happendb/proto/gen/go/happendb/store/v1"
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

	pbStore.RegisterReadOnlyServiceServer(srv.grpcServer, srv)
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

// ReadEvents ...
func (s StoreServer) ReadEvents(req *pbStore.ReadEventsRequest, stream pbStore.ReadOnlyService_ReadEventsServer) error {
	eventsChannel, err := s.readOnlyStore.ReadEvents(req.GetAggregateId())

	if err != nil {
		return err
	}

	for event := range eventsChannel {
		stream.Send(event.Event)
	}

	log.WithField("request", req).Debugf("%T::ReadEvents\n", s)

	return nil
}

// Append ...
func (s StoreServer) Append(ctx context.Context, req *pbStore.AppendRequest) (*pbStore.AppendResponse, error) {
	err := s.writeOnlyStore.Append(req.GetStreamName(), messaging.WrapN(req.GetEvents())...)

	if err != nil {
		return nil, err
	}

	log.WithField("request", req).Debugf("%T::Append\n", s)

	return &pbStore.AppendResponse{}, nil
}
