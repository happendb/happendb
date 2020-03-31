package driver

import (
	"database/sql"
	"fmt"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/happendb/happendb/pkg/store"
	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
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
func (d *Postgres) Append(streamName string, events ...*pbMessaging.Event) error {
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
			log.Error().AnErr("could execute insert query", err)
		}
	}

	txn.Commit()

	return nil
}

// ReadStreamEventsForward ...
func (d *Postgres) ReadStreamEventsForward(aggregateID string, offset uint64, limit uint64) ([]*pbMessaging.Event, error) {
	events := make([]*pbMessaging.Event, 0)

	eventCh, err := d.ReadStreamEventsForwardAsync(aggregateID, offset, limit)

	if err != nil {
		return nil, err
	}

	for event := range eventCh {
		events = append(events, event)
	}

	return events, nil
}

// ReadStreamEventsForwardAsync ...
func (d *Postgres) ReadStreamEventsForwardAsync(aggregateID string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error) {
	var (
		err       error
		rows      *sql.Rows
		tableName string
	)

	if tableName, err = d.generateTableName(store.PersistModeSingleTable, aggregateID); err != nil {
		return nil, fmt.Errorf("could not generate table name: %v", err)
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	log.Debug().Str("query", q).Msgf("[%T::ReadStreamEventsForwardAsync] querying count", d)

	r := d.db.QueryRow(q)

	if err != nil {
		return nil, fmt.Errorf("could not execute query: %v", err)
	}

	var count int64
	r.Scan(&count)

	q = fmt.Sprintf("SELECT * FROM %s ORDER BY version", tableName)
	log.Debug().Str("query", q).Msgf("[%T::ReadStreamEventsForwardAsync] querying rows", d)

	if rows, err = d.db.Query(q); err != nil {
		return nil, fmt.Errorf("could not execute query: %v", err)
	}

	events := make([]*pbMessaging.Event, 0)

	for rows.Next() {
		event := &pbMessaging.Event{
			Payload:  &any.Any{},
			Metadata: &any.Any{},
		}

		if err := rows.Scan(&event.Id, &event.Type, &event.Payload.Value, &event.Metadata.Value, &event.Version, &event.Time); err != nil {
			return nil, fmt.Errorf("could not scan rows: %v", err)
		}

		events = append(events, event)
	}

	ch := make(chan *pbMessaging.Event, count)

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
