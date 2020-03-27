package postgres

import (
	"database/sql"
	"fmt"

	"github.com/happendb/happendb/pkg/messaging"
	"github.com/happendb/happendb/pkg/store"
	"github.com/labstack/gommon/log"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// Driver ...
type Driver struct {
	db *sql.DB
}

// NewPostgresDriver ...
func NewPostgresDriver() (*Driver, error) {
	db, err := sql.Open("postgres", "host=localhost dbname=happendb user=postgres password=123 sslmode=disable")

	if err != nil {
		return nil, err
	}

	return &Driver{
		db,
	}, nil
}

// Append ...
func (d *Driver) Append(streamName string, events ...*messaging.Event) error {
	var (
		err       error
		tableName string
	)

	txn, err := d.db.Begin()

	if err != nil {
		return err
	}

	if tableName, err = generateTableName(store.PersistModeSingleTable, streamName); err != nil {
		return err
	}

	for _, event := range events {
		_, err := d.db.Exec(
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

// ReadEvents ...
func (d *Driver) ReadEvents(aggregateID string) (*messaging.EventStream, error) {
	var (
		err       error
		rows      *sql.Rows
		tableName string
	)

	// TODO(daniel): Grab this persist mode via configurable options of the driver
	if tableName, err = generateTableName(store.PersistModeSingleTable, aggregateID); err != nil {
		return nil, err
	}

	if rows, err = d.db.Query(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1", tableName), aggregateID); err != nil {
		return nil, err
	}

	events := make([]*messaging.Event, 0)

	for rows.Next() {
		event := messaging.NewEvent()

		if err := rows.Scan(&event.Id, &event.Type, &event.AggregateId, &event.Payload.Value); err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return messaging.NewEventStream(aggregateID, messaging.UnwrapN(events)...), nil
}

func generateTableName(persistMode store.PersistMode, streamName string) (string, error) {
	switch persistMode {
	case store.PersistModeSingleTable:
		return "events", nil
	default:
		return "", store.ErrInvalidTableName
	}
}
