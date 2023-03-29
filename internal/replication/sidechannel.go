package replication

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"github.com/noctarius/event-stream-prototype/internal/replication/channel"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"time"
)

const addTableToPublication = "ALTER PUBLICATION %s ADD TABLE %s"
const dropTableFromPublication = "ALTER PUBLICATION %s DROP TABLE %s"

type sideChannel struct {
	connConfig        *pgx.ConnConfig
	publicationName   string
	snapshotBatchSize int
}

func newSideChannel(connConfig *pgx.ConnConfig, publicationName string, snapshotBatchSize int) *sideChannel {
	sc := &sideChannel{
		connConfig:        connConfig,
		publicationName:   publicationName,
		snapshotBatchSize: snapshotBatchSize,
	}
	return sc
}

func (sc *sideChannel) QueryFunc(ctx context.Context, fn channel.RowFunction, query string, args ...any) error {
	return sc.QueryFuncWithTimeout(ctx, time.Second*20, fn, query, args...)
}

func (sc *sideChannel) QueryFuncWithTimeout(ctx context.Context, timeout time.Duration,
	fn channel.RowFunction, query string, args ...any) error {

	connection, err := pgx.ConnectConfig(context.Background(), sc.connConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}
	defer connection.Close(context.Background())

	return sc.queryFuncWithTimeout(connection, ctx, timeout, fn, query, args...)
}

func (sc *sideChannel) NewSession(fn func(adapter channel.QuerySession) error) error {
	connection, err := pgx.ConnectConfig(context.Background(), sc.connConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}
	defer connection.Close(context.Background())

	return fn(&sessionAdapter{connection: connection})
}

func (sc *sideChannel) AttachChunkToPublication(chunk *model.Chunk) error {
	canonicalChunkName := chunk.CanonicalName()
	attachingQuery := fmt.Sprintf(addTableToPublication, sc.publicationName, canonicalChunkName)
	return sc.NewSession(func(session channel.QuerySession) error {
		if _, err := session.Exec(context.Background(), attachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		logger.Printf("Updated publication %s to add table %s", sc.publicationName, canonicalChunkName)
		return nil
	})
}

func (sc *sideChannel) DetachChunkFromPublication(chunk *model.Chunk) error {
	canonicalChunkName := chunk.CanonicalName()
	detachingQuery := fmt.Sprintf(dropTableFromPublication, sc.publicationName, canonicalChunkName)
	return sc.NewSession(func(session channel.QuerySession) error {
		if _, err := session.Exec(context.Background(), detachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		logger.Printf("Updated publication %s to drop table %s", sc.publicationName, canonicalChunkName)
		return nil
	})
}

func (sc *sideChannel) SnapshotTable(canonicalName string, startingLSN *pglogrepl.LSN,
	cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error) {

	var currentLSN pglogrepl.LSN

	err := sc.NewSession(func(session channel.QuerySession) error {
		if _, err := session.Exec(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {

			return err
		}

		if err := session.QueryRow(context.Background(),
			"SELECT pg_current_wal_lsn()").Scan(&currentLSN); err != nil {

			return err
		}

		if startingLSN != nil {
			if _, err := session.Exec(context.Background(),
				fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", startingLSN.String()),
			); err != nil {
				return err
			}
		}

		if _, err := session.Exec(context.Background(),
			fmt.Sprintf("DECLARE clone SCROLL CURSOR FOR SELECT * FROM %s", canonicalName)); err != nil {
			return errors.Wrap(err, 0)
		}

		var rowDecoder *decoding.RowDecoder
		for {
			count := 0
			if err := session.QueryFunc(context.Background(), func(row pgx.Row) error {
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

		_, err := session.Exec(context.Background(), "CLOSE clone")
		if err != nil {
			return errors.Wrap(err, 0)
		}

		_, err = session.Exec(context.Background(), "ROLLBACK")
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

func (sc *sideChannel) queryFuncWithTimeout(connection *pgx.Conn, ctx context.Context, timeout time.Duration,
	fn channel.RowFunction, query string, args ...any) error {

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rows, err := connection.Query(ctx, query, args...)
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

func (sc *sideChannel) queryExecWithTimeout(connection *pgx.Conn, ctx context.Context,
	timeout time.Duration, query string, args ...any) (pgconn.CommandTag, error) {

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return connection.Exec(ctx, query, args...)
}

func (sc *sideChannel) queryRowWithTimeout(connection *pgx.Conn, ctx context.Context,
	timeout time.Duration, query string, args ...any) pgx.Row {

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return connection.QueryRow(ctx, query, args...)
}

type sessionAdapter struct {
	sideChannel *sideChannel
	connection  *pgx.Conn
}

func (s *sessionAdapter) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return s.sideChannel.queryExecWithTimeout(s.connection, ctx, time.Second*20, query, args...)
}

func (s *sessionAdapter) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return s.sideChannel.queryRowWithTimeout(s.connection, ctx, time.Second*20, query, args...)
}

func (s *sessionAdapter) QueryFunc(ctx context.Context, fn channel.RowFunction, query string, args ...any) error {
	return s.sideChannel.queryFuncWithTimeout(s.connection, ctx, time.Second*20, fn, query, args...)
}

func (s *sessionAdapter) QueryFuncWithTimeout(ctx context.Context, timeout time.Duration,
	fn channel.RowFunction, query string, args ...any) error {

	return s.sideChannel.queryFuncWithTimeout(s.connection, ctx, timeout, fn, query, args...)
}
