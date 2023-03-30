package channels

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"time"
)

const addTableToPublication = "ALTER PUBLICATION %s ADD TABLE %s"

const dropTableFromPublication = "ALTER PUBLICATION %s DROP TABLE %s"

const initialHypertableQuery = `
SELECT h1.id, h1.schema_name, h1.table_name, h1.associated_schema_name, h1.associated_table_prefix, 
	 h1.compression_state, h1.compressed_hypertable_id, coalesce(h2.is_distributed, false)
FROM _timescaledb_catalog.hypertable h1
LEFT JOIN timescaledb_information.hypertables h2 
	 ON h2.hypertable_schema = h1.schema_name 
	AND h2.hypertable_name = h1.table_name`

const initialChunkQuery = `
SELECT c1.id, c1.hypertable_id, c1.schema_name, c1.table_name, c1.compressed_chunk_id, c1.dropped, c1.status
FROM _timescaledb_catalog.chunk c1
LEFT JOIN timescaledb_information.chunks c2
       ON c2.chunk_schema = c1.schema_name
      AND c2.chunk_name = c1.table_name
LEFT JOIN _timescaledb_catalog.chunk c3 ON c3.compressed_chunk_id = c1.id
LEFT JOIN timescaledb_information.chunks c4
       ON c4.chunk_schema = c3.schema_name
      AND c4.chunk_name = c3.table_name
ORDER BY c1.hypertable_id, coalesce(c2.range_start, c4.range_start)`

const initialTableSchema = `
SELECT
   c.column_name,
   t.oid::int,
   CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END,
   CASE WHEN c.is_identity = 'YES' THEN true ELSE false END,
   c.column_default
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = c.udt_schema
LEFT JOIN pg_catalog.pg_type t ON t.typnamespace = n.oid AND t.typname = c.udt_name
WHERE c.table_schema = '%s'
  AND c.table_name = '%s'
ORDER BY c.ordinal_position`

type sideChannel struct {
	connConfig        *pgx.ConnConfig
	publicationName   string
	snapshotBatchSize int
}

func NewSideChannel(connConfig *pgx.ConnConfig, publicationName string, snapshotBatchSize int) SideChannel {
	sc := &sideChannel{
		connConfig:        connConfig,
		publicationName:   publicationName,
		snapshotBatchSize: snapshotBatchSize,
	}
	return sc
}

func (sc *sideChannel) ReadHypertables(cb func(hypertable *model.Hypertable) error) error {
	return sc.newSession(func(session session) error {
		return session.queryFunc(context.Background(), func(row pgx.Row) error {
			var id int32
			var schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string
			var compressionState int16
			var compressedHypertableId *int32
			var distributed bool

			if err := row.Scan(&id, &schemaName, &hypertableName, &associatedSchemaName,
				&associatedTablePrefix, &compressionState, &compressedHypertableId, &distributed); err != nil {
				return errors.Wrap(err, 0)
			}

			hypertable := model.NewHypertable(id, sc.connConfig.Database, schemaName, hypertableName,
				associatedSchemaName, associatedTablePrefix, compressedHypertableId, compressionState, distributed)

			logger.Printf("ADDED CATALOG ENTRY: HYPERTABLE %d => %+v", hypertable.Id(), hypertable)
			return cb(hypertable)
		}, initialHypertableQuery)
	})
}

func (sc *sideChannel) ReadChunks(cb func(chunk *model.Chunk) error) error {
	return sc.newSession(func(session session) error {
		return session.queryFunc(context.Background(), func(row pgx.Row) error {
			var id, hypertableId int32
			var schemaName, tableName string
			var compressedChunkId *int32
			var dropped bool
			var status int32

			if err := row.Scan(&id, &hypertableId, &schemaName, &tableName,
				&compressedChunkId, &dropped, &status); err != nil {
				return errors.Wrap(err, 0)
			}

			return cb(model.NewChunk(id, hypertableId, schemaName, tableName, dropped, status, compressedChunkId))
		}, initialChunkQuery)
	})
}

func (sc *sideChannel) ReadHypertablesSchema(hypertables []*model.Hypertable,
	cb func(hypertable *model.Hypertable, columns []model.Column) bool) error {

	return sc.newSession(func(session session) error {
		for _, hypertable := range hypertables {
			if hypertable.SchemaName() == "_timescaledb_internal" ||
				hypertable.SchemaName() == "_timescaledb_catalog" {

				continue
			}
			return sc.readHypertableSchema(session, hypertable, cb)
		}
		return nil
	})
}

func (sc *sideChannel) ReadHypertableSchema(hypertable *model.Hypertable,
	cb func(hypertable *model.Hypertable, columns []model.Column) bool) error {

	if hypertable.SchemaName() != "_timescaledb_internal" &&
		hypertable.SchemaName() != "_timescaledb_catalog" {

		return sc.newSession(func(session session) error {
			return sc.readHypertableSchema(session, hypertable, cb)
		})
	}
	return nil
}

func (sc *sideChannel) AttachChunkToPublication(chunk *model.Chunk) error {
	canonicalChunkName := chunk.CanonicalName()
	attachingQuery := fmt.Sprintf(addTableToPublication, sc.publicationName, canonicalChunkName)
	return sc.newSession(func(session session) error {
		if _, err := session.exec(context.Background(), attachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		logger.Printf("Updated publication %s to add table %s", sc.publicationName, canonicalChunkName)
		return nil
	})
}

func (sc *sideChannel) DetachChunkFromPublication(chunk *model.Chunk) error {
	canonicalChunkName := chunk.CanonicalName()
	detachingQuery := fmt.Sprintf(dropTableFromPublication, sc.publicationName, canonicalChunkName)
	return sc.newSession(func(session session) error {
		if _, err := session.exec(context.Background(), detachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		logger.Printf("Updated publication %s to drop table %s", sc.publicationName, canonicalChunkName)
		return nil
	})
}

func (sc *sideChannel) SnapshotTable(canonicalName string, startingLSN *pglogrepl.LSN,
	cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error) {

	var currentLSN pglogrepl.LSN

	err := sc.newSession(func(session session) error {
		if _, err := session.exec(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {

			return err
		}

		if err := session.queryRow(context.Background(),
			"SELECT pg_current_wal_lsn()").Scan(&currentLSN); err != nil {

			return err
		}

		if startingLSN != nil {
			if _, err := session.exec(context.Background(),
				fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", startingLSN.String()),
			); err != nil {
				return err
			}
		}

		if _, err := session.exec(context.Background(),
			fmt.Sprintf("DECLARE clone SCROLL CURSOR FOR SELECT * FROM %s", canonicalName)); err != nil {
			return errors.Wrap(err, 0)
		}

		var rowDecoder *decoding.RowDecoder
		for {
			count := 0
			if err := session.queryFunc(context.Background(), func(row pgx.Row) error {
				rows := row.(pgx.Rows)

				if rowDecoder == nil {
					rd, err := decoding.NewRowDecoder(rows.FieldDescriptions())
					if err != nil {
						return err
					}
					rowDecoder = rd
				}

				return rowDecoder.DecodeMapAndSink(rows.RawValues(), func(values map[string]any) error {
					count++
					return cb(currentLSN, values)
				})
			}, fmt.Sprintf("FETCH FORWARD %d FROM clone", sc.snapshotBatchSize)); err != nil {
				return errors.Wrap(err, 0)
			}
			if count == 0 || count < sc.snapshotBatchSize {
				break
			}
		}

		_, err := session.exec(context.Background(), "CLOSE clone")
		if err != nil {
			return errors.Wrap(err, 0)
		}

		_, err = session.exec(context.Background(), "ROLLBACK")
		if err != nil {
			return errors.Wrap(err, 0)
		}
		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err, 0)
	}

	return currentLSN, nil
}

func (sc *sideChannel) InitialSnapshot(snapshotName string, next func() (schema, table string, ok bool)) error {
	conn, err := pgx.ConnectConfig(context.Background(), sc.connConfig)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err = conn.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return err
	}

	if _, err = conn.Exec(context.Background(),
		fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
		return err
	}

	for {
		schemaName, tableName, ok := next()
		if !ok {
			break
		}

		regClass := fmt.Sprintf("%s.%s", schemaName, tableName)
		if _, err := conn.Exec(
			context.Background(),
			fmt.Sprintf("DECLARE clone SCROLL CURSOR FOR SELECT * FROM %s", regClass),
		); err != nil {
			return errors.Wrap(err, 0)
		}

		for {
			rows, err := conn.Query(context.Background(), "FETCH FORWARD 10 FROM clone")
			if err != nil {
				return errors.Wrap(err, 0)
			}
			count := 0
			if err := decoding.DecodeRowValues(rows, func(values []any) error {
				logger.Printf("%+v", values)
				count++
				return nil
			}); err != nil {
				return errors.Wrap(err, 0)
			}
			if count == 0 {
				break
			}
		}
		_, err = conn.Exec(context.Background(), "CLOSE clone")
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	_, err = conn.Exec(context.Background(), "ROLLBACK")
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (sc *sideChannel) readHypertableSchema(
	session session, hypertable *model.Hypertable,
	cb func(hypertable *model.Hypertable, columns []model.Column) bool) error {

	columns := make([]model.Column, 0)
	if err := session.queryFunc(context.Background(), func(row pgx.Row) error {
		var name string
		var oid uint32
		var nullable, identifier bool
		var defaultValue *string

		if err := row.Scan(&name, &oid, &nullable, &identifier, &defaultValue); err != nil {
			return errors.Wrap(err, 0)
		}

		dataType, err := model.DataTypeByOID(oid)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		column := model.NewColumn(name, oid, string(dataType), nullable, identifier, defaultValue)
		columns = append(columns, column)
		return nil
	}, fmt.Sprintf(initialTableSchema, hypertable.SchemaName(), hypertable.HypertableName())); err != nil {
		return errors.Wrap(err, 0)
	}

	if !cb(hypertable, columns) {
		return errors.Errorf("hypertable schema callback failed")
	}

	return nil
}

func (sc *sideChannel) newSession(fn func(session session) error) error {
	connection, err := pgx.ConnectConfig(context.Background(), sc.connConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}
	defer connection.Close(context.Background())

	return fn(&sessionAdapter{connection: connection})
}

type sessionAdapter struct {
	sideChannel *sideChannel
	connection  *pgx.Conn
}

func (s *sessionAdapter) exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return s.queryExecWithTimeout(s.connection, ctx, time.Second*20, query, args...)
}

func (s *sessionAdapter) queryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return s.queryRowWithTimeout(ctx, time.Second*20, query, args...)
}

func (s *sessionAdapter) queryFunc(ctx context.Context, fn rowFunction, query string, args ...any) error {
	return s.queryFuncWithTimeout(ctx, time.Second*20, fn, query, args...)
}

func (s *sessionAdapter) queryFuncWithTimeout(ctx context.Context, timeout time.Duration,
	fn rowFunction, query string, args ...any) error {

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rows, err := s.connection.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (s *sessionAdapter) queryRowWithTimeout(ctx context.Context,
	timeout time.Duration, query string, args ...any) pgx.Row {

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return s.connection.QueryRow(ctx, query, args...)
}

func (s *sessionAdapter) queryExecWithTimeout(connection *pgx.Conn, ctx context.Context,
	timeout time.Duration, query string, args ...any) (pgconn.CommandTag, error) {

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return connection.Exec(ctx, query, args...)
}
