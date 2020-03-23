package postgres

import (
	"database/sql"
	"fmt"

	"github.com/happendb/happendb/pkg/messaging"
	"github.com/happendb/happendb/pkg/store"
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

	stream := messaging.NewEventStream(aggregateID)

	for rows.Next() {
		event := messaging.NewEvent()

		if err := rows.Scan(&event.Id, &event.Type, &event.AggregateId, &event.Payload.Value); err != nil {
			return nil, err
		}

		stream.Append(event)
	}

	return stream, nil
}

func generateTableName(persistMode store.PersistMode, _ string) (string, error) {
	switch persistMode {
	case store.PersistModeSingleTable:
		return "events", nil
	default:
		return "", store.ErrInvalidTableName
	}
}
