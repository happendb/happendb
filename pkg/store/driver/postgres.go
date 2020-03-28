package driver

import (
	"database/sql"
	"fmt"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/happendb/happendb/pkg/messaging"
	"github.com/happendb/happendb/pkg/store"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// Postgres ...
type Postgres struct {
	db *sql.DB
}

// NewPostgresDriver ...
func NewPostgresDriver() (*Postgres, error) {
	db, err := sql.Open("postgres", "host=localhost dbname=happendb user=postgres password=123 sslmode=disable")

	if err != nil {
		return nil, err
	}

	return &Postgres{db}, nil
}

// Append ...
func (d Postgres) Append(streamName string, events ...*messaging.Event) error {
	var (
		err       error
		tableName string
		txn       *sql.Tx
	)

	if txn, err = d.db.Begin(); err != nil {
		return fmt.Errorf("could not begin transaction: %v", err)
	}

	if tableName, err = d.generateTableName(store.PersistModeSingleTable, streamName); err != nil {
		return fmt.Errorf("could not generate table name: %v", err)
	}

	for _, event := range events {
		_, err := d.db.Exec(
			fmt.Sprintf("INSERT INTO %s(id, type, payload, time) VALUES ($1, $2, $3, $4)", tableName),
			event.GetId(),
			event.GetType(),
			string(event.Payload.Value),
			event.GetTime(),
		)

		if err, ok := err.(*pq.Error); ok {
			log.Error(err)
		}
	}

	txn.Commit()

	return nil
}

// ReadEvents ...
func (d Postgres) ReadEvents(aggregateID string) (<-chan *messaging.Event, error) {
	var (
		err       error
		rows      *sql.Rows
		tableName string
	)

	if tableName, err = d.generateTableName(store.PersistModeSingleTable, aggregateID); err != nil {
		return nil, fmt.Errorf("could not generate table name: %v", err)
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	log.WithField("query", q).Debugf("%T::ReadEvents", d)

	r := d.db.QueryRow(q)

	if err != nil {
		return nil, fmt.Errorf("could not execute query: %v", err)
	}

	var count int64
	r.Scan(&count)

	q = fmt.Sprintf("SELECT * FROM %s", tableName)
	log.WithField("query", q).Debugf("%T::ReadEvents", d)

	if rows, err = d.db.Query(q); err != nil {
		return nil, fmt.Errorf("could not execute query: %v", err)
	}

	events := []*messaging.Event{}

	for rows.Next() {
		event := messaging.NewEvent(&any.Any{}, &any.Any{})

		if err := rows.Scan(&event.Id, &event.Type, &event.Payload.Value, &event.Metadata.Value, &event.Version, &event.Time); err != nil {
			return nil, fmt.Errorf("could not scan rows: %v", err)
		}

		events = append(events, event)
	}

	ch := make(chan *messaging.Event, count)

	for _, e := range events {
		ch <- e
	}

	close(ch)

	return ch, nil
}

func (d *Postgres) generateTableName(persistMode store.PersistMode, streamName string) (string, error) {
	switch persistMode {
	case store.PersistModeSingleTable:
		return pq.QuoteIdentifier(fmt.Sprintf("events_%s", streamName)), nil
	default:
		return "", store.ErrInvalidTableName
	}
}
