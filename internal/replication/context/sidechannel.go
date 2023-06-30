/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package context

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/timescaledb-event-streamer/internal/pgdecoding"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
	"strings"
	"time"
)

type Grant string

const (
	Select     Grant = "select"
	Insert     Grant = "insert"
	Update     Grant = "update"
	Delete     Grant = "delete"
	Truncate   Grant = "truncate"
	References Grant = "references"
	Trigger    Grant = "trigger"
)

type readReplicationSlotFunc = func(slotName string) (
	pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
)

const getSystemInformationQuery = `
SELECT current_database(), pcs.system_identifier, pcc.timeline_id 
FROM pg_control_system() pcs, pg_control_checkpoint() pcc`

const addTableToPublicationQuery = "ALTER PUBLICATION %s ADD TABLE %s"

const dropTableFromPublicationQuery = "ALTER PUBLICATION %s DROP TABLE %s"

const initialHypertableQuery = `
SELECT h1.id, h1.schema_name, h1.table_name, h1.associated_schema_name, h1.associated_table_prefix,
	 h1.compression_state, h1.compressed_hypertable_id, coalesce(h2.is_distributed, false),
	 ca.user_view_schema, ca.user_view_name, c.relreplident
FROM _timescaledb_catalog.hypertable h1
LEFT JOIN timescaledb_information.hypertables h2
	 ON h2.hypertable_schema = h1.schema_name
	AND h2.hypertable_name = h1.table_name
LEFT JOIN _timescaledb_catalog.continuous_agg ca
    ON h1.id = ca.mat_hypertable_id
LEFT JOIN pg_catalog.pg_namespace n
    ON n.nspname = h1.schema_name
LEFT JOIN pg_catalog.pg_class c
    ON c.relname = h1.table_name
   AND c.relnamespace = n.oid;
`

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
   CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END AS nullable,
   coalesce(p.indisprimary, false) AS is_primary_key,
   p.key_seq,
   c.column_default,
   coalesce(p.indisreplident, false) AS is_replica_ident,
   p.index_name,
   CASE o.option & 1 WHEN 1 THEN 'DESC' ELSE 'ASC' END AS index_column_order,
   CASE o.option & 2 WHEN 2 THEN 'NULLS FIRST' ELSE 'NULLS LAST' END AS index_nulls_order,
   d.column_name IS NOT NULL AS is_dimension,
   coalesce(d.aligned, false) AS dim_aligned,
   CASE WHEN d.interval_length IS NULL THEN 'space' ELSE 'time' END AS dim_type,
   CASE WHEN d.column_name IS NOT NULL THEN rank() over (order by d.id) END
FROM information_schema.columns c
LEFT JOIN (
    SELECT
        cl.relname,
        n.nspname,
        a.attname,
        (information_schema._pg_expandarray(i.indkey)).n AS key_seq,
        (information_schema._pg_expandarray(i.indkey)) AS keys,
        a.attnum,
        i.indisreplident,
        i.indisprimary,
        cl2.relname AS index_name,
        i.indoption
    FROM pg_index i, pg_attribute a, pg_class cl, pg_namespace n, pg_class cl2
    WHERE cl.relnamespace = n.oid
      AND a.attrelid = cl.oid
      AND i.indrelid = cl.oid
      AND i.indexrelid = cl2.oid
      AND i.indisprimary
) p ON p.attname = c.column_name AND p.nspname = c.table_schema AND p.relname = c.table_name AND p.attnum = (p.keys).x
LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = c.udt_schema
LEFT JOIN pg_catalog.pg_type t ON t.typnamespace = n.oid AND t.typname = c.udt_name
LEFT JOIN unnest (p.indoption) WITH ORDINALITY o (option, ordinality) ON p.attnum = o.ordinality
LEFT JOIN _timescaledb_catalog.hypertable h ON h.schema_name = c.table_schema AND h.table_name = c.table_name
LEFT JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id AND d.column_name = c.column_name
WHERE c.table_schema = $1
  AND c.table_name = $2
ORDER BY c.ordinal_position`

const snapshotHighWatermarkQuery = `
SELECT %s
FROM %s
ORDER BY %s
LIMIT 1`

const replicaIdentityQuery = `
SELECT c.relreplident::text
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname=$1 and c.relname=$2`

const hypertableContinuousAggregateQuery = `
SELECT ca.user_view_schema, ca.user_view_name
FROM _timescaledb_catalog.continuous_agg ca 
WHERE ca.mat_hypertable_id = $1`

const readReplicationSlotQuery = `
SELECT plugin, slot_type, restart_lsn, confirmed_flush_lsn
FROM pg_catalog.pg_replication_slots prs
WHERE slot_name = $1`

const checkExistingReplicationSlotQuery = `
SELECT true
FROM pg_catalog.pg_replication_slots prs
WHERE slot_name = $1`

const createPublicationQuery = "SELECT create_timescaledb_catalog_publication($1, $2)"

const checkExistingPublicationQuery = "SELECT true FROM pg_publication WHERE pubname = $1"

const dropPublicationQuery = "DROP PUBLICATION IF EXISTS %s"

const checkExistingPublicationPublishedTablesQuery = `
SELECT pt.schemaname, pt.tablename
FROM pg_catalog.pg_publication_tables pt
WHERE pt.pubname = $1`

const checkExistingTableInPublicationQuery = `
SELECT true
FROM pg_catalog.pg_publication_tables pt
WHERE pt.pubname = $1
  AND pt.schemaname = $2
  AND pt.tablename = $3`

const timescaledbVersionQuery = `
SELECT extversion
FROM pg_catalog.pg_extension
WHERE extname = 'timescaledb'`

const postgresqlVersionQuery = `SHOW SERVER_VERSION`

const walLevelQuery = `SHOW WAL_LEVEL`

const checkTablePrivilegeByUserQuery = `SELECT HAS_TABLE_PRIVILEGE($1, $2, $3)`

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

func (sc *sideChannelImpl) hasTablePrivilege(username string,
	entity systemcatalog.SystemEntity, grant Grant) (access bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(
			checkTablePrivilegeByUserQuery,
			username,
			entity.CanonicalName(),
			string(grant),
		).Scan(&access)
	})
	return
}

func (sc *sideChannelImpl) createPublication(publicationName string) (success bool, err error) {
	err = sc.newSession(time.Second*10, func(session *session) error {
		err = session.queryRow(
			createPublicationQuery,
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
		return session.queryRow(checkExistingPublicationQuery, publicationName).Scan(&found)
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	return
}

func (sc *sideChannelImpl) dropPublication(publicationName string) error {
	return sc.newSession(time.Second*10, func(session *session) error {
		_, err := session.exec(fmt.Sprintf(dropPublicationQuery, publicationName))
		if e, ok := err.(*pgconn.PgError); ok {
			if e.Code == pgerrcode.UndefinedObject {
				return nil
			}
		}
		if err == nil {
			sc.logger.Infof("Dropped publication %s", publicationName)
		}
		return err
	})
}

func (sc *sideChannelImpl) existsTableInPublication(
	publicationName, schemaName, tableName string) (found bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(checkExistingTableInPublicationQuery, publicationName, schemaName, tableName).Scan(&found)
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	return
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
			var replicaIdentity pgtypes.ReplicaIdentity

			if err := row.Scan(&id, &schemaName, &hypertableName, &associatedSchemaName,
				&associatedTablePrefix, &compressionState, &compressedHypertableId,
				&distributed, &viewSchema, &viewName, &replicaIdentity); err != nil {

				return errors.Wrap(err, 0)
			}

			hypertable := systemcatalog.NewHypertable(id, sc.replicationContext.DatabaseName(), schemaName,
				hypertableName, associatedSchemaName, associatedTablePrefix, compressedHypertableId,
				compressionState, distributed, viewSchema, viewName, replicaIdentity)

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

func (sc *sideChannelImpl) snapshotChunkTable(chunk *systemcatalog.Chunk, snapshotBatchSize int,
	cb func(lsn pgtypes.LSN, values map[string]any) error) (pgtypes.LSN, error) {

	var currentLSN pgtypes.LSN = 0

	cursorName := supporting.RandomTextString(15)
	cursorQuery := fmt.Sprintf(
		"DECLARE %s SCROLL CURSOR FOR SELECT * FROM %s", cursorName, chunk.CanonicalName(),
	)

	if err := sc.snapshotTableWithCursor(
		cursorQuery, cursorName, nil, snapshotBatchSize,
		func(lsn pgtypes.LSN, values map[string]any) error {
			if currentLSN == 0 {
				currentLSN = lsn
			}
			return cb(lsn, values)
		},
	); err != nil {
		return 0, errors.Wrap(err, 0)
	}

	return currentLSN, nil
}

func (sc *sideChannelImpl) fetchHypertableSnapshotBatch(
	hypertable *systemcatalog.Hypertable, snapshotName string, snapshotBatchSize int,
	cb func(lsn pgtypes.LSN, values map[string]any) error) error {

	index, present := hypertable.Columns().SnapshotIndex()
	if !present {
		return errors.Errorf("missing snapshotting index for hypertable '%s'", hypertable.CanonicalName())
	}

	return sc.replicationContext.SnapshotContextTransaction(
		snapshotName, false,
		func(snapshotContext *watermark.SnapshotContext) error {
			watermark, present := snapshotContext.GetWatermark(hypertable)
			if !present {
				return errors.Errorf("illegal watermark state for hypertable '%s'", hypertable.CanonicalName())
			}

			comparison, success := index.WhereTupleLE(watermark.HighWatermark())
			if !success {
				return errors.Errorf("failed encoding watermark: %+v", watermark.HighWatermark())
			}

			if watermark.HasValidLowWatermark() {
				lowWatermarkComparison, success := index.WhereTupleGT(watermark.LowWatermark())
				if !success {
					return errors.Errorf("failed encoding watermark: %+v", watermark.LowWatermark())
				}

				sc.logger.Debugf(
					"Resuming snapshotting of hypertable '%s' at <<%s>> up to <<%s>>",
					hypertable.CanonicalName(), lowWatermarkComparison, comparison,
				)

				comparison = fmt.Sprintf("%s AND %s",
					lowWatermarkComparison,
					comparison,
				)
			} else {
				sc.logger.Debugf(
					"Starting snapshotting of hypertable '%s' up to <<%s>>",
					hypertable.CanonicalName(), comparison,
				)
			}

			cursorName := supporting.RandomTextString(15)
			cursorQuery := fmt.Sprintf(
				`DECLARE %s SCROLL CURSOR FOR SELECT * FROM %s WHERE %s ORDER BY %s LIMIT %d`,
				cursorName, hypertable.CanonicalName(), comparison,
				index.AsSqlOrderBy(false), snapshotBatchSize*10,
			)

			hook := func(lsn pgtypes.LSN, values map[string]any) error {
				indexValues := supporting.FilterMap(values, func(key string, _ any) bool {
					for _, column := range index.Columns() {
						if column.Name() == key {
							return true
						}
					}
					return false
				})

				watermark.SetLowWatermark(indexValues)
				return cb(lsn, values)
			}

			return sc.snapshotTableWithCursor(cursorQuery, cursorName, &snapshotName, snapshotBatchSize, hook)
		},
	)
}

func (sc *sideChannelImpl) snapshotTableWithCursor(
	cursorQuery, cursorName string, snapshotName *string, snapshotBatchSize int,
	cb func(lsn pgtypes.LSN, values map[string]any) error) error {

	return sc.newSession(time.Minute*60, func(session *session) error {
		if _, err := session.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return err
		}

		if snapshotName != nil {
			if _, err := session.exec(
				fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", *snapshotName),
			); err != nil {
				return errors.Wrap(err, 0)
			}
		}

		var currentLSN pglogrepl.LSN
		if err := session.queryRow("SELECT pg_current_wal_lsn()").Scan(&currentLSN); err != nil {
			return errors.Wrap(err, 0)
		}

		// Declare cursor for fetch iterations
		if _, err := session.exec(cursorQuery); err != nil {
			return errors.Wrap(err, 0)
		}

		var rowDecoder *pgdecoding.RowDecoder
		for {
			count := 0
			if err := session.queryFunc(func(row pgx.Row) error {
				rows := row.(pgx.Rows)

				if rowDecoder == nil {
					// Initialize the row decoder based on the returned field descriptions
					rd, err := pgdecoding.NewRowDecoder(rows.FieldDescriptions())
					if err != nil {
						return errors.Wrap(err, 0)
					}
					rowDecoder = rd
				}

				return rowDecoder.DecodeMapAndSink(rows.RawValues(), func(values map[string]any) error {
					count++
					return cb(pgtypes.LSN(currentLSN), values)
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
}

func (sc *sideChannelImpl) readSnapshotHighWatermark(
	hypertable *systemcatalog.Hypertable, snapshotName string) (values map[string]any, err error) {

	index, present := hypertable.Columns().SnapshotIndex()
	if !present {
		return nil, errors.Errorf(
			"missing snapshotting index for hypertable '%s'", hypertable.CanonicalName(),
		)
	}

	query := fmt.Sprintf(
		snapshotHighWatermarkQuery, index.AsSqlTuple(), hypertable.CanonicalName(), index.AsSqlOrderBy(true),
	)
	if err := sc.newSession(time.Second*10, func(session *session) error {
		if _, err := session.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return err
		}

		if _, err := session.exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
			return errors.Wrap(err, 0)
		}

		return session.queryFunc(func(row pgx.Row) error {
			rows := row.(pgx.Rows)

			rowDecoder, err := pgdecoding.NewRowDecoder(rows.FieldDescriptions())
			if err != nil {
				return errors.Wrap(err, 0)
			}

			return rowDecoder.DecodeMapAndSink(rows.RawValues(), func(decoded map[string]any) error {
				values = decoded
				return nil
			})
		}, query)

	}); err != nil {
		return nil, err
	}
	return
}

func (sc *sideChannelImpl) readReplicaIdentity(schemaName, tableName string) (pgtypes.ReplicaIdentity, error) {
	var replicaIdentity pgtypes.ReplicaIdentity
	if err := sc.newSession(time.Second*10, func(session *session) error {
		row := session.queryRow(replicaIdentityQuery, schemaName, tableName)

		var val string
		if err := row.Scan(&val); err != nil {
			return errors.Wrap(err, 0)
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
		}, checkExistingPublicationPublishedTablesQuery, publicationName)
	}); err != nil {
		return nil, err
	}
	return systemEntities, nil
}

func (sc *sideChannelImpl) readReplicationSlot(
	slotName string,
) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error) {
	err = sc.newSession(time.Second*10, func(session *session) error {
		var restart, confirmed string
		if err := session.queryRow(readReplicationSlotQuery, slotName).Scan(
			&pluginName, &slotType, &restart, &confirmed,
		); err != nil {
			return err
		}
		lsn, err := pglogrepl.ParseLSN(restart)
		if err != nil {
			return err
		}
		restartLsn = pgtypes.LSN(lsn)
		lsn, err = pglogrepl.ParseLSN(confirmed)
		if err != nil {
			return err
		}
		confirmedFlushLsn = pgtypes.LSN(lsn)
		return nil
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	return
}

func (sc *sideChannelImpl) existsReplicationSlot(slotName string) (found bool, err error) {
	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(checkExistingReplicationSlotQuery, slotName).Scan(&found)
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
		var name, sortOrder, nullsOrder string
		var oid uint32
		var keySeq, dimSeq *int
		var nullable, primaryKey, isReplicaIdent, dimension, dimAligned bool
		var defaultValue, indexName, dimType *string

		if err := row.Scan(&name, &oid, &nullable, &primaryKey, &keySeq,
			&defaultValue, &isReplicaIdent, &indexName, &sortOrder, &nullsOrder,
			&dimension, &dimAligned, &dimType, &dimSeq); err != nil {

			return errors.Wrap(err, 0)
		}

		dataType, err := systemcatalog.DataTypeByOID(oid)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		column := systemcatalog.NewIndexColumn(
			name, oid, string(dataType), nullable, primaryKey, keySeq, defaultValue, isReplicaIdent, indexName,
			systemcatalog.IndexSortOrder(sortOrder), systemcatalog.IndexNullsOrder(nullsOrder),
			dimension, dimAligned, dimType, dimSeq,
		)
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
