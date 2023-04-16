package context

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/timescaledb-event-streamer/internal/pgdecoding"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"strings"
	"time"
)

const getSystemInformationQuery = `
SELECT current_database(), pcs.system_identifier, pcc.timeline_id 
FROM pg_control_system() pcs, pg_control_checkpoint() pcc`

const addTableToPublicationQuery = "ALTER PUBLICATION %s ADD TABLE %s"

const dropTableFromPublicationQuery = "ALTER PUBLICATION %s DROP TABLE %s"

const initialHypertableQuery = `
SELECT h1.id, h1.schema_name, h1.table_name, h1.associated_schema_name, h1.associated_table_prefix,
	 h1.compression_state, h1.compressed_hypertable_id, coalesce(h2.is_distributed, false),
	 ca.user_view_schema, ca.user_view_name
FROM _timescaledb_catalog.hypertable h1
LEFT JOIN timescaledb_information.hypertables h2
	 ON h2.hypertable_schema = h1.schema_name
	AND h2.hypertable_name = h1.table_name
LEFT JOIN _timescaledb_catalog.continuous_agg ca
    ON h1.id = ca.mat_hypertable_id`

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

const initialTableSchemaQuery = `
SELECT
   c.column_name,
   t.oid::int,
   CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END,
   CASE WHEN p.attname IS NOT NULL THEN true ELSE false END,
   c.column_default
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = c.udt_schema
LEFT JOIN pg_catalog.pg_type t ON t.typnamespace = n.oid AND t.typname = c.udt_name
LEFT JOIN LATERAL (
    SELECT a.attname
    FROM pg_index i, pg_attribute a, pg_class cl, pg_namespace n
    WHERE cl.relname = c.table_name
      AND n.nspname = c.table_schema
      AND cl.relnamespace = n.oid
      AND a.attrelid = cl.oid
      AND i.indrelid = cl.oid
      AND a.attname = c.column_name
      AND a.attnum = any(i.indkey)
      AND i.indisprimary
) p ON TRUE
WHERE c.table_schema = $1
  AND c.table_name = $2
ORDER BY c.ordinal_position`

const replicaIdentityQuery = `
SELECT c.relreplident
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname=$1 and c.relname=$2
`

const hypertableContinuousAggregateQuery = `
SELECT ca.user_view_schema, ca.user_view_name
FROM _timescaledb_catalog.continuous_agg ca 
WHERE ca.mat_hypertable_id = $1`

const readReplicationSlot = `
SELECT plugin, slot_type, restart_lsn, confirmed_flush_lsn
FROM pg_catalog.pg_replication_slots prs
WHERE slot_name = $1`

const createPublication = "SELECT create_timescaledb_catalog_publication($1, $2)"

const checkExistingPublication = "SELECT true FROM pg_publication WHERE pubname = $1"

const dropPublication = "DROP PUBLICATION IF EXISTS %s"

const existingPublicationPublishedTablesQuery = `
SELECT pt.schemaname, pt.tablename
FROM pg_catalog.pg_publication_tables pt
WHERE pt.pubname = $1`

const timescaledbVersionQuery = `
SELECT extversion
FROM pg_catalog.pg_extension
WHERE extname = 'timescaledb'`

const postgresqlVersionQuery = `SHOW SERVER_VERSION`

const walLevelQuery = `SHOW WAL_LEVEL`

type sideChannelImpl struct {
	logger             *logging.Logger
	replicationContext *ReplicationContext
}

func newSideChannel(replicationContext *ReplicationContext) (*sideChannelImpl, error) {
	logger, err := logging.NewLogger("SideChannel")
	if err != nil {
		return nil, err
	}

	return &sideChannelImpl{
		logger:             logger,
		replicationContext: replicationContext,
	}, nil
}

func (sc *sideChannelImpl) createPublication(publicationName string) (success bool, err error) {
	err = sc.newSession(time.Second*10, func(session *session) error {
		err = session.queryRow(
			createPublication,
			publicationName,
			sc.replicationContext.DatabaseUsername(),
		).Scan(&success)
		if err != nil {
			return err
		}
		sc.logger.Infof("Created publication %s", publicationName)
		return nil
	})
	return
}

func (sc *sideChannelImpl) existsPublication(publicationName string) (found bool, err error) {
	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(checkExistingPublication, publicationName).Scan(&found)
	})
	return
}

func (sc *sideChannelImpl) dropPublication(publicationName string) error {
	return sc.newSession(time.Second*10, func(session *session) error {
		_, err := session.exec(fmt.Sprintf(dropPublication, publicationName))
		if e, ok := err.(*pgconn.PgError); ok {
			if e.Code == "42704" {
				return nil
			}
		}
		if err == nil {
			sc.logger.Infof("Dropped publication %s", publicationName)
		}
		return err
	})
}

func (sc *sideChannelImpl) getSystemInformation() (databaseName, systemId string, timeline int32, err error) {
	if err := sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(getSystemInformationQuery).Scan(&databaseName, &systemId, &timeline)
	}); err != nil {
		return databaseName, systemId, timeline, err
	}
	return
}

func (sc *sideChannelImpl) getWalLevel() (walLevel string, err error) {
	walLevel = "unknown"
	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(walLevelQuery).Scan(&walLevel)
	})
	return
}

func (sc *sideChannelImpl) getPostgresVersion() (pgVersion version.PostgresVersion, err error) {
	if err = sc.newSession(time.Second*10, func(session *session) error {
		var v string
		if err := session.queryRow(postgresqlVersionQuery).Scan(&v); err != nil {
			return err
		}
		pgVersion, err = version.ParsePostgresVersion(v)
		return nil
	}); err != nil {
		return
	}
	return
}

func (sc *sideChannelImpl) getTimescaleDBVersion() (tsdbVersion version.TimescaleVersion, found bool, err error) {
	if err = sc.newSession(time.Second*10, func(session *session) error {
		var v string
		if err := session.queryRow(timescaledbVersionQuery).Scan(&v); err != nil {
			return err
		}
		tsdbVersion, err = version.ParseTimescaleVersion(v)
		found = true
		return nil
	}); err != nil {
		if err == pgx.ErrNoRows {
			err = nil
		}
		return 0, false, err
	}
	return
}

func (sc *sideChannelImpl) readHypertables(cb func(hypertable *systemcatalog.Hypertable) error) error {
	return sc.newSession(time.Second*20, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			var id int32
			var schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string
			var compressionState int16
			var compressedHypertableId *int32
			var distributed bool
			var viewSchema, viewName *string

			if err := row.Scan(&id, &schemaName, &hypertableName, &associatedSchemaName,
				&associatedTablePrefix, &compressionState, &compressedHypertableId,
				&distributed, &viewSchema, &viewName); err != nil {

				return errors.Wrap(err, 0)
			}

			hypertable := systemcatalog.NewHypertable(id, sc.replicationContext.DatabaseName(), schemaName,
				hypertableName, associatedSchemaName, associatedTablePrefix, compressedHypertableId,
				compressionState, distributed, viewSchema, viewName)

			return cb(hypertable)
		}, initialHypertableQuery)
	})
}

func (sc *sideChannelImpl) readChunks(cb func(chunk *systemcatalog.Chunk) error) error {
	return sc.newSession(time.Second*20, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			var id, hypertableId int32
			var schemaName, tableName string
			var compressedChunkId *int32
			var dropped bool
			var status int32

			if err := row.Scan(&id, &hypertableId, &schemaName, &tableName,
				&compressedChunkId, &dropped, &status); err != nil {
				return errors.Wrap(err, 0)
			}

			return cb(
				systemcatalog.NewChunk(id, hypertableId, schemaName, tableName, dropped, status, compressedChunkId),
			)
		}, initialChunkQuery)
	})
}

func (sc *sideChannelImpl) readHypertableSchema(
	cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool,
	hypertables ...*systemcatalog.Hypertable) error {

	return sc.newSession(time.Second*10, func(session *session) error {
		for _, hypertable := range hypertables {
			if (!hypertable.IsContinuousAggregate() && hypertable.SchemaName() == "_timescaledb_internal") ||
				hypertable.SchemaName() == "_timescaledb_catalog" {

				continue
			}
			if err := sc.readHypertableSchema0(session, hypertable, cb); err != nil {
				return err
			}
		}
		return nil
	})
}

func (sc *sideChannelImpl) attachTablesToPublication(
	publicationName string, entities ...systemcatalog.SystemEntity) error {

	if len(entities) == 0 {
		return nil
	}

	entityTableList := sc.entitiesToTableList(entities)
	attachingQuery := fmt.Sprintf(addTableToPublicationQuery, publicationName, entityTableList)
	return sc.newSession(time.Second*20, func(session *session) error {
		if _, err := session.exec(attachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		for _, entity := range entities {
			sc.logger.Infof("Updated publication %s to add table %s", publicationName, entity.CanonicalName())
		}
		return nil
	})
}

func (sc *sideChannelImpl) detachTablesFromPublication(
	publicationName string, entities ...systemcatalog.SystemEntity) error {

	if len(entities) == 0 {
		return nil
	}

	entityTableList := sc.entitiesToTableList(entities)
	detachingQuery := fmt.Sprintf(dropTableFromPublicationQuery, publicationName, entityTableList)
	return sc.newSession(time.Second*20, func(session *session) error {
		if _, err := session.exec(detachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		for _, entity := range entities {
			sc.logger.Infof("Updated publication %s to drop table %s", publicationName, entity.CanonicalName())
		}
		return nil
	})
}

func (sc *sideChannelImpl) snapshotTable(canonicalName string, startingLSN *pglogrepl.LSN, snapshotBatchSize int,
	cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error) {

	var currentLSN pglogrepl.LSN

	err := sc.newSession(time.Minute*60, func(session *session) error {
		if _, err := session.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return err
		}

		if err := session.queryRow("SELECT pg_current_wal_lsn()").Scan(&currentLSN); err != nil {
			return err
		}

		if startingLSN != nil {
			if _, err := session.exec(
				fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", startingLSN.String()),
			); err != nil {
				return err
			}
		}

		cursorName := supporting.RandomTextString(15)
		if _, err := session.exec(
			fmt.Sprintf("DECLARE %s SCROLL CURSOR FOR SELECT * FROM %s", cursorName, canonicalName),
		); err != nil {
			return errors.Wrap(err, 0)
		}

		var rowDecoder *pgdecoding.RowDecoder
		for {
			count := 0
			if err := session.queryFunc(func(row pgx.Row) error {
				rows := row.(pgx.Rows)

				if rowDecoder == nil {
					rd, err := pgdecoding.NewRowDecoder(rows.FieldDescriptions())
					if err != nil {
						return err
					}
					rowDecoder = rd
				}

				return rowDecoder.DecodeMapAndSink(rows.RawValues(), func(values map[string]any) error {
					count++
					return cb(currentLSN, values)
				})
			}, fmt.Sprintf("FETCH FORWARD %d FROM %s", snapshotBatchSize, cursorName)); err != nil {
				return errors.Wrap(err, 0)
			}
			if count == 0 || count < snapshotBatchSize {
				break
			}
			session.ResetTimeout(time.Minute * 60)
		}

		_, err := session.exec(fmt.Sprintf("CLOSE %s", cursorName))
		if err != nil {
			return errors.Wrap(err, 0)
		}

		_, err = session.exec("ROLLBACK")
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

func (sc *sideChannelImpl) readReplicaIdentity(schemaName, tableName string) (pgtypes.ReplicaIdentity, error) {
	var replicaIdentity pgtypes.ReplicaIdentity
	if err := sc.newSession(time.Second*10, func(session *session) error {
		row := session.queryRow(replicaIdentityQuery, schemaName, tableName)

		var val string
		if err := row.Scan(&val); err != nil {
			return err
		}
		replicaIdentity = pgtypes.AsReplicaIdentity(val)
		return nil
	}); err != nil {
		return pgtypes.UNKNOWN, err
	}
	return replicaIdentity, nil
}

func (sc *sideChannelImpl) readContinuousAggregate(materializedHypertableId int32) (string, string, bool, error) {
	var viewSchema, viewName string

	found := false
	if err := sc.newSession(time.Second*10, func(session *session) error {
		row := session.queryRow(hypertableContinuousAggregateQuery, materializedHypertableId)
		if err := row.Scan(&viewSchema, &viewName); err != nil {
			if err != pgx.ErrNoRows {
				return err
			}
		} else {
			found = true
		}
		return nil
	}); err != nil {
		return "", "", false, err
	}
	return viewSchema, viewName, found, nil
}

func (sc *sideChannelImpl) readPublishedTables(publicationName string) ([]systemcatalog.SystemEntity, error) {
	systemEntities := make([]systemcatalog.SystemEntity, 0)
	if err := sc.newSession(time.Second*20, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			var schemaName, tableName string
			if err := row.Scan(&schemaName, &tableName); err != nil {
				return err
			}
			systemEntities = append(systemEntities, systemcatalog.NewSystemEntity(schemaName, tableName))
			return nil
		}, existingPublicationPublishedTablesQuery, publicationName)
	}); err != nil {
		return nil, err
	}
	return systemEntities, nil
}

func (sc *sideChannelImpl) readReplicationSlot(
	slotName string,
) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error) {
	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(readReplicationSlot, slotName).Scan(
			&pluginName, &slotType, &restartLsn, &confirmedFlushLsn,
		)
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	return
}

func (sc *sideChannelImpl) readHypertableSchema0(
	session *session, hypertable *systemcatalog.Hypertable,
	cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool) error {

	columns := make([]systemcatalog.Column, 0)
	if err := session.queryFunc(func(row pgx.Row) error {
		var name string
		var oid uint32
		var nullable, primaryKey bool
		var defaultValue *string

		if err := row.Scan(&name, &oid, &nullable, &primaryKey, &defaultValue); err != nil {
			return errors.Wrap(err, 0)
		}

		dataType, err := systemcatalog.DataTypeByOID(oid)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		column := systemcatalog.NewColumn(name, oid, string(dataType), nullable, primaryKey, defaultValue)
		columns = append(columns, column)
		return nil
	}, initialTableSchemaQuery, hypertable.SchemaName(), hypertable.TableName()); err != nil {
		return errors.Wrap(err, 0)
	}

	if !cb(hypertable, columns) {
		return errors.Errorf("hypertable schema callback failed")
	}

	return nil
}

func (sc *sideChannelImpl) entitiesToTableList(entities []systemcatalog.SystemEntity) string {
	if len(entities) == 1 {
		return entities[0].CanonicalName()
	}

	canonicalEntityNames := make([]string, len(entities))
	for i, entity := range entities {
		canonicalEntityNames[i] = entity.CanonicalName()
	}

	return strings.Join(canonicalEntityNames, ",")
}

func (sc *sideChannelImpl) newSession(timeout time.Duration, fn func(session *session) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	connection, err := sc.replicationContext.newSideChannelConnection(ctx)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}
	defer connection.Close(context.Background())

	s := &session{
		connection: connection,
		ctx:        ctx,
		cancel:     cancel,
	}

	defer func() {
		s.cancel()
	}()

	return fn(s)
}

type rowFunction = func(row pgx.Row) error

type session struct {
	connection *pgx.Conn
	ctx        context.Context
	cancel     func()
}

func (s *session) ResetTimeout(timeout time.Duration) {
	// Cancel old context
	s.cancel()

	// Initialize new timeout
	s.ctx, s.cancel = context.WithTimeout(context.Background(), timeout)
}

func (s *session) queryFunc(fn rowFunction, query string, args ...any) error {
	rows, err := s.connection.Query(s.ctx, query, args...)
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

func (s *session) queryRow(query string, args ...any) pgx.Row {
	return s.connection.QueryRow(s.ctx, query, args...)
}

func (s *session) exec(query string, args ...any) (pgconn.CommandTag, error) {
	return s.connection.Exec(s.ctx, query, args...)
}
