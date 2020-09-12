package driver

import (
	"database/sql"
	"fmt"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"

	pbMessaging "github.com/happendb/happendb/proto/gen/go/happendb/messaging/v1"
	"github.com/happendb/happendb/store"
)

// EventStreamsTableName ...
const EventStreamsTableName = "event_streams"

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

// PostgresDriver ...
type PostgresDriver struct {
	db *sql.DB
}

// NewPostgresDriver ...
func NewPostgresDriver(db *sql.DB) (*PostgresDriver, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PostgresDriver{
		db,
	}, nil
}

// ReadEventsForward ...
func (d *PostgresDriver) ReadEventsForward(aggregateID string, offset uint64, limit uint64) ([]*pbMessaging.Event, error) {
	events := make([]*pbMessaging.Event, 0)

	eventCh, err := d.ReadEventsForwardAsync(aggregateID, offset, limit)

	if err != nil {
		return nil, err
	}

	for event := range eventCh {
		events = append(events, event)
	}

	return events, nil
}

// ReadEventsForwardAsync ...
func (d *PostgresDriver) ReadEventsForwardAsync(aggregateID string, offset uint64, limit uint64) (<-chan *pbMessaging.Event, error) {
	var (
		err       error
		rows      *sql.Rows
		tableName string
	)

	tableName, err = d.generateTableName(aggregateID)

	if err != nil {
		return nil, fmt.Errorf("could not generate table name: %v", err)
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", q).Msg("[ReadEventsForwardAsync] querying count")

	r := d.db.QueryRow(q)

	if err != nil {
		return nil, fmt.Errorf("could not execute event count query: %v", err)
	}

	var count int64
	r.Scan(&count)

	q = fmt.Sprintf("SELECT * FROM %s ORDER BY version", pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", q).Msg("[ReadEventsForwardAsync] querying rows")

	if rows, err = d.db.Query(q); err != nil {
		return nil, fmt.Errorf("could not execute events query: %v", err)
	}

	events := make([]*pbMessaging.Event, 0)

	for rows.Next() {
		event := &pbMessaging.Event{
			Payload:  &structpb.Struct{},
			Metadata: &structpb.Struct{},
		}

		if err := rows.Scan(&event.Id, &event.Type, &event.Payload, &event.Metadata, &event.Version, &event.Time); err != nil {
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

// Append ...
func (d *PostgresDriver) Append(streamName string, version uint64, events ...*pbMessaging.Event) error {
	var (
		err       error
		tableName string
		txn       *sql.Tx
	)

	exists, err := d.StreamExists(streamName)

	if !exists {
		_, _ = d.CreateStream(streamName)
	}

	if txn, err = d.db.Begin(); err != nil {
		return fmt.Errorf("could not begin transaction: %v", err)
	}

	if tableName, err = d.generateTableName(streamName); err != nil {
		return fmt.Errorf("could not generate table name: %v", err)
	}

	q := fmt.Sprintf(insertEventSQLf, pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", q).Msg("[Append] inserting event")

	for i, event := range events {
		_, err := d.db.Exec(
			q,
			event.GetId(),
			event.GetType(),
			string(event.Payload.String()),
			string(event.Metadata.String()),
			1+int(version)+i,
			event.GetTime(),
		)

		if err != nil {
			if err, ok := err.(*pq.Error); ok {
				log.Debug().Msg(err.Error())
				return err
			}
		}
	}

	if err = txn.Commit(); err != nil {
		return err
	}

	return nil
}

// CreateStream ...
func (d *PostgresDriver) CreateStream(streamName string) (*store.Stream, error) {
	if streamName == "" {
		return nil, store.ErrInvalidStreamName
	}

	tableName, _ := d.generateTableName(streamName)
	query := fmt.Sprintf(createStreamSQLf, pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", query).Msg("[Append] creating stream table")

	if _, err := d.db.Exec(query); err != nil {
		return nil, err
	}

	return store.NewStream(streamName), nil
}

// StreamExists ...
func (d *PostgresDriver) StreamExists(streamName string) (bool, error) {
	tableName, err := d.generateTableName(streamName)

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

// DeleteStream ...
func (d *PostgresDriver) DeleteStream(streamName string) error {
	tableName, err := d.generateTableName(streamName)

	if err != nil {
		return fmt.Errorf("could not generate table name: %v", err)
	}

	query := fmt.Sprintf("DROP TABLE %s.%s", "public", pq.QuoteIdentifier(tableName))
	log.Debug().Str("query", query).Msg("[Append] deleting stream table")

	if _, err = d.db.Exec(query); err != nil {
		return fmt.Errorf("could not execute delete stream query: %v", err)
	}

	return nil
}

func (d *PostgresDriver) generateTableName(streamName string) (string, error) {
	return fmt.Sprintf("events_%s", streamName), nil
}
