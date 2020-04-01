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

const (
	createStreamSQLf = `
CREATE TABLE %s (
    "id" uuid NOT NULL,
    "type" character varying(255) NOT NULL,
    "payload" jsonb NOT NULL,
    "metadata" jsonb,
    "version" bigint NOT NULL,
	"time" timestamptz NOT NULL
);
`

	insertEventSQLf = `
INSERT INTO %s(id, type, payload, metadata, version, time) VALUES ($1, $2, $3, $4, $5, $6);
	`
)

// Postgres ...
type Postgres struct {
	db      *sql.DB
	streams map[string]*sql.Rows
}

// NewPostgresDriver ...
func NewPostgresDriver() (*Postgres, error) {
	db, err := sql.Open("postgres", "host=localhost dbname=happendb user=postgres password=123 sslmode=disable")

	if err != nil {
		return nil, err
	}

	return &Postgres{db: db}, nil
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

	q := fmt.Sprintf(insertEventSQLf, pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", q).Msgf("[%T::Append] inserting event", d)

	for _, event := range events {
		_, err := d.db.Exec(
			q,
			event.GetId(),
			event.GetType(),
			string(event.Payload.GetValue()),
			string(event.Metadata.GetValue()),
			event.GetVersion(),
			event.GetTime(),
		)

		if err, ok := err.(*pq.Error); ok {
			return err
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

	hasStream, err := d.HasStream(aggregateID)

	if err != nil {
		return nil, fmt.Errorf("could not check if stream exists: %v", err)
	}

	if !hasStream {
		stream, err := d.CreateStream(aggregateID)

		if err != nil {
			return nil, fmt.Errorf("could not create stream: %v", err)
		}

		_ = stream
	}

	tableName, err = d.generateTableName(store.PersistModeSingleTable, aggregateID)

	if err != nil {
		return nil, fmt.Errorf("could not generate table name: %v", err)
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", q).Msgf("[%T::ReadStreamEventsForwardAsync] querying count", d)

	r := d.db.QueryRow(q)

	if err != nil {
		return nil, fmt.Errorf("could not execute event count query: %v", err)
	}

	var count int64
	r.Scan(&count)

	q = fmt.Sprintf("SELECT * FROM %s ORDER BY version", pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", q).Msgf("[%T::ReadStreamEventsForwardAsync] querying rows", d)

	if rows, err = d.db.Query(q); err != nil {
		return nil, fmt.Errorf("could not execute events query: %v", err)
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
		return fmt.Sprintf("events_%s", streamName), nil
	default:
		return "", store.ErrInvalidTableName
	}
}

// CreateStream ...
func (d *Postgres) CreateStream(streamName string) (*store.Stream, error) {
	tableName, err := d.generateTableName(store.PersistModeSingleTable, streamName)

	if err != nil {
		return nil, fmt.Errorf("could not generate table name: %v", err)
	}

	query := fmt.Sprintf(createStreamSQLf, pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", query).Msgf("[%T::Append] creating stream table", d)

	_, err = d.db.Exec(query)

	if err, ok := err.(*pq.Error); ok {
		return nil, fmt.Errorf("could not execute create stream query: %v", err)
	}

	return store.NewStream(tableName, nil), nil
}

// HasStream ...
func (d *Postgres) HasStream(streamName string) (bool, error) {
	tableName, err := d.generateTableName(store.PersistModeSingleTable, streamName)

	if err != nil {
		return false, fmt.Errorf("could not generate table name: %v", err)
	}

	q := fmt.Sprintf(`
SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = '%s'
   AND    table_name   = '%s'
);
	`, "public", tableName)

	var exists bool
	err = d.db.QueryRow(q).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf("could not execute query: %v", err)
	}

	return exists, nil
}
