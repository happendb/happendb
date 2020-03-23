package postgres

import (
	"database/sql"
	"fmt"

	"github.com/golang/protobuf/ptypes/any"
	messaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/labstack/gommon/log"

	"github.com/happendb/happendb/pkg/store"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// StoreOption ...
type StoreOption = func(*Store)

// WithPersistMode ...
func WithPersistMode(m store.PersistMode) StoreOption {
	return func(s *Store) {
		s.persistMode = m
	}
}

// Store ...
type Store struct {
	db          *sql.DB
	persistMode store.PersistMode
}

// NewPostgresStore ...
func NewPostgresStore(opts ...StoreOption) (*Store, error) {
	db, err := sql.Open("postgres", "host=localhost dbname=happendb user=postgres password=123 sslmode=disable")

	if err != nil {
		return nil, err
	}

	store := &Store{
		db: db,
	}

	for _, opt := range opts {
		opt(store)
	}

	return store, nil
}

// ReadEvents ...
func (s *Store) ReadEvents(aggregateID string) (*messaging.EventStream, error) {
	var (
		err       error
		rows      *sql.Rows
		tableName string
	)

	if tableName, err = generateTableName(s.persistMode, aggregateID); err != nil {
		return nil, err
	}

	if rows, err = s.db.Query(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1", tableName), aggregateID); err != nil {
		return nil, err
	}

	stream := &messaging.EventStream{
		Name:   aggregateID,
		Events: make([]*messaging.Event, 0),
	}

	for rows.Next() {
		event := &messaging.Event{
			Payload: &any.Any{},
		}

		if err := rows.Scan(&event.Id, &event.Type, &event.AggregateId, &event.Payload.Value); err != nil {
			return nil, err
		}

		stream.Events = append(stream.Events, event)
	}

	return stream, nil
}

// Append ...
func (s *Store) Append(streamName string, events ...*messaging.Event) error {
	var (
		err       error
		tableName string
	)

	txn, err := s.db.Begin()

	if err != nil {
		return err
	}

	if tableName, err = generateTableName(store.PersistModeSingleTable, streamName); err != nil {
		return err
	}

	for _, event := range events {
		_, err := s.db.Exec(
			fmt.Sprintf("INSERT INTO %s(id, type, aggregate_id, payload) VALUES ($1, $2, $3, $4)", tableName),
			event.GetId(),
			event.GetType(),
			event.GetAggregateId(),
			string(event.Payload.Value),
		)

		if err, ok := err.(*pq.Error); ok {
			log.Error(err)
		}
	}

	txn.Commit()

	return nil
}

func generateTableName(persistMode store.PersistMode, _ string) (string, error) {
	switch persistMode {
	case store.PersistModeSingleTable:
		return "events", nil
	default:
		return "", store.ErrInvalidTableName
	}
}
