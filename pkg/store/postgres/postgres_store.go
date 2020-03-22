package postgres

import (
	"database/sql"

	"github.com/happendb/happendb/pkg/messaging"
	_ "github.com/lib/pq"
)

// Store ...
type Store struct {
	db *sql.DB
}

// NewPostgresStore ...
func NewPostgresStore() (*Store, error) {
	db, err := sql.Open("postgres", "host=localhost dbname=happendb user=postgres password=123 sslmode=disable")

	if err != nil {
		return nil, err
	}

	return &Store{
		db,
	}, nil
}

// ReadEvents ...
func (s *Store) ReadEvents(aggregateID string) ([]*messaging.Event, error) {
	var (
		err    error
		rows   *sql.Rows
		events = make([]*messaging.Event, 0)
	)

	if rows, err = s.db.Query("SELECT * FROM events WHERE aggregate_id = $1", aggregateID); err != nil {
		return nil, err
	}

	for rows.Next() {
		event := messaging.NewEvent()

		if err := rows.Scan(&event.Id, &event.Type, &event.AggregateID, &event.Payload.Value); err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, nil
}
