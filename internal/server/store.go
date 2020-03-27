package server

import (
	"context"
	"net"

	"github.com/happendb/happendb/pkg/messaging"
	"github.com/happendb/happendb/pkg/store"
	"github.com/happendb/happendb/pkg/store/postgres"
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
	driver, err := postgres.NewPostgresDriver()

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
	lis, err := net.Listen("tcp", ":3000")

	if err != nil {
		return err
	}

	return s.grpcServer.Serve(lis)
}

// ReadEvents ...
func (s *StoreServer) ReadEvents(ctx context.Context, req *pbStore.ReadEventsRequest) (res *pbStore.ReadEventsResponse, err error) {
	var stream *messaging.EventStream

	if stream, err = s.readOnlyStore.ReadEvents(req.GetAggregateId()); err != nil {
		return
	}

	log.WithFields(log.Fields{"req": req}).Debugf("%T::ReadEvents\n", s)

	res = &pbStore.ReadEventsResponse{
		AggregateId: req.GetAggregateId(),
		EventStream: stream.EventStream,
	}

	return
}

// Append ...
func (s *StoreServer) Append(ctx context.Context, req *pbStore.AppendRequest) (res *pbStore.AppendResponse, err error) {
	res = &pbStore.AppendResponse{}
	err = s.writeOnlyStore.Append(req.GetStream().GetName(), messaging.WrapN(req.GetEvents())...)

	if err != nil {
		return
	}

	log.WithFields(log.Fields{"req": req}).Debugf("%T::Append\n", s)

	return
}
