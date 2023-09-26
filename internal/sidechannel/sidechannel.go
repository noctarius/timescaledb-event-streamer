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

package sidechannel

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
	"github.com/samber/lo"
	"strings"
	"time"
)

type sideChannel struct {
	logger              *logging.Logger
	pgxConfig           *pgx.ConnConfig
	stateStorageManager statestorage.Manager
}

func NewSideChannel(
	stateStorageManager statestorage.Manager, pgxConfig *pgx.ConnConfig,
) (sidechannel.SideChannel, error) {

	logger, err := logging.NewLogger("SideChannel")
	if err != nil {
		return nil, err
	}

	return &sideChannel{
		logger:              logger,
		pgxConfig:           pgxConfig,
		stateStorageManager: stateStorageManager,
	}, nil
}

func (sc *sideChannel) HasTablePrivilege(
	username string, entity systemcatalog.SystemEntity, grant sidechannel.TableGrant,
) (access bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(
			queryCheckUserTablePrivilege,
			username,
			entity.CanonicalName(),
			string(grant),
		).Scan(&access)
	})
	return
}

func (sc *sideChannel) CreatePublication(
	publicationName string,
) (success bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		err = session.queryRow(
			queryCreatePublication,
			publicationName,
			sc.pgxConfig.User,
		).Scan(&success)
		if err != nil {
			return err
		}
		sc.logger.Infof("Created publication %s", publicationName)
		return nil
	})
	if err != nil {
		err = errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) ExistsPublication(
	publicationName string,
) (found bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(queryCheckPublicationExists, publicationName).Scan(&found)
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	if err != nil {
		err = errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) DropPublication(
	publicationName string,
) error {

	return sc.newSession(time.Second*10, func(session *session) error {
		_, err := session.exec(fmt.Sprintf(queryTemplateDropPublication, publicationName))
		if e, ok := err.(*pgconn.PgError); ok {
			if e.Code == pgerrcode.UndefinedObject {
				return nil
			}
		}
		if err == nil {
			sc.logger.Infof("Dropped publication %s", publicationName)
		}
		if err != nil {
			err = errors.Wrap(err, 0)
		}
		return err
	})
}

func (sc *sideChannel) ExistsTableInPublication(
	publicationName, schemaName, tableName string,
) (found bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(queryCheckTableExistsInPublication, publicationName, schemaName, tableName).Scan(&found)
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	if err != nil {
		err = errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) GetSystemInformation() (databaseName, systemId string, timeline int32, err error) {
	if err := sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(queryReadSystemInformation).Scan(&databaseName, &systemId, &timeline)
	}); err != nil {
		return databaseName, systemId, timeline, errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) GetWalLevel() (walLevel string, err error) {
	walLevel = "unknown"
	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(queryConfiguredWalLevel).Scan(&walLevel)
	})
	if err != nil {
		err = errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) GetPostgresVersion() (pgVersion version.PostgresVersion, err error) {
	if err = sc.newSession(time.Second*10, func(session *session) error {
		var v string
		if err := session.queryRow(queryPostgreSqlVersion).Scan(&v); err != nil {
			return err
		}
		pgVersion, err = version.ParsePostgresVersion(v)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) GetTimescaleDBVersion() (tsdbVersion version.TimescaleVersion, found bool, err error) {
	if err = sc.newSession(time.Second*10, func(session *session) error {
		var v string
		if err := session.queryRow(queryTimescaleDbVersion).Scan(&v); err != nil {
			return err
		}
		tsdbVersion, err = version.ParseTimescaleVersion(v)
		if err != nil {
			return err
		}
		found = true
		return nil
	}); err != nil {
		if err == pgx.ErrNoRows {
			err = nil
		}
		if err != nil {
			err = errors.Wrap(err, 0)
		}
		return 0, false, err
	}
	return
}

func (sc *sideChannel) ReadVanillaTables(
	cb func(table *systemcatalog.PgTable) error,
) error {

	return sc.newSession(time.Second*20, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			var relId uint32
			var schemaName, tableName string
			var replicaIdentity pgtypes.ReplicaIdentity

			if err := row.Scan(&relId, &schemaName, &tableName, &replicaIdentity); err != nil {
				return errors.Wrap(err, 0)
			}

			return cb(
				systemcatalog.NewPgTable(relId, schemaName, tableName, replicaIdentity),
			)
		}, queryReadVanillaTables)
	})
}

func (sc *sideChannel) ReadHypertables(
	cb func(hypertable *systemcatalog.Hypertable) error,
) error {

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

			hypertable := systemcatalog.NewHypertable(
				id, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix,
				compressedHypertableId, compressionState, distributed, viewSchema, viewName, replicaIdentity,
			)

			return cb(hypertable)
		}, queryReadHypertables)
	})
}

func (sc *sideChannel) ReadChunks(
	cb func(chunk *systemcatalog.Chunk) error,
) error {

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
		}, queryReadChunks)
	})
}

func (sc *sideChannel) ReadVanillaTableSchema(
	cb sidechannel.TableSchemaCallback,
	pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
	tables ...*systemcatalog.PgTable,
) error {

	return sc.newSession(time.Second*10, func(session *session) error {
		for _, table := range tables {
			if err := sc.readVanillaTableSchema0(session, table, pgTypeResolver, cb); err != nil {
				return errors.Wrap(err, 0)
			}
		}
		return nil
	})
}

func (sc *sideChannel) ReadHypertableSchema(
	cb sidechannel.TableSchemaCallback, pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
	hypertables ...*systemcatalog.Hypertable,
) error {

	return sc.newSession(time.Second*10, func(session *session) error {
		for _, hypertable := range hypertables {
			if (!hypertable.IsContinuousAggregate() && hypertable.SchemaName() == "_timescaledb_internal") ||
				hypertable.SchemaName() == "_timescaledb_catalog" {

				continue
			}
			if err := sc.readHypertableSchema0(session, hypertable, pgTypeResolver, cb); err != nil {
				return errors.Wrap(err, 0)
			}
		}
		return nil
	})
}

func (sc *sideChannel) AttachTablesToPublication(
	publicationName string, entities ...systemcatalog.SystemEntity,
) error {

	if len(entities) == 0 {
		return nil
	}

	entityTableList := sc.entitiesToTableList(entities)
	attachingQuery := fmt.Sprintf(queryTemplateAddTableToPublication, publicationName, entityTableList)
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

func (sc *sideChannel) DetachTablesFromPublication(
	publicationName string, entities ...systemcatalog.SystemEntity,
) error {

	if len(entities) == 0 {
		return nil
	}

	entityTableList := sc.entitiesToTableList(entities)
	detachingQuery := fmt.Sprintf(queryTemplateDropTableFromPublication, publicationName, entityTableList)
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

func (sc *sideChannel) SnapshotChunkTable(
	rowDecoderFactory pgtypes.RowDecoderFactory, chunk *systemcatalog.Chunk,
	snapshotBatchSize int, cb sidechannel.SnapshotRowCallback,
) (pgtypes.LSN, error) {

	var currentLSN pgtypes.LSN = 0

	cursorName := lo.RandomString(15, lo.LowerCaseLettersCharset)
	cursorQuery := fmt.Sprintf(
		"DECLARE %s SCROLL CURSOR FOR SELECT * FROM %s", cursorName, chunk.CanonicalName(),
	)

	callback := func(lsn pgtypes.LSN, values map[string]any) error {
		if currentLSN == 0 {
			currentLSN = lsn
		}
		return cb(lsn, values)
	}

	if err := sc.snapshotTableWithCursor(
		rowDecoderFactory, cursorQuery, cursorName, nil, snapshotBatchSize, callback,
	); err != nil {
		return 0, errors.Wrap(err, 0)
	}

	return currentLSN, nil
}

func (sc *sideChannel) FetchTableSnapshotBatch(
	rowDecoderFactory pgtypes.RowDecoderFactory, table systemcatalog.BaseTable,
	snapshotName string, snapshotBatchSize int, cb sidechannel.SnapshotRowCallback,
) error {

	index, present := table.Columns().SnapshotIndex()
	if !present {
		return errors.Errorf("missing snapshotting index for table '%s'", table.CanonicalName())
	}

	return sc.stateStorageManager.SnapshotContextTransaction(
		snapshotName, false,
		func(snapshotContext *watermark.SnapshotContext) error {
			tableWatermark, present := snapshotContext.GetWatermark(table)
			if !present {
				return errors.Errorf("illegal watermark state for table '%s'", table.CanonicalName())
			}

			comparison, success := index.WhereTupleLE(tableWatermark.HighWatermark())
			if !success {
				return errors.Errorf("failed encoding watermark: %+v", tableWatermark.HighWatermark())
			}

			if tableWatermark.HasValidLowWatermark() {
				lowWatermarkComparison, success := index.WhereTupleGT(tableWatermark.LowWatermark())
				if !success {
					return errors.Errorf("failed encoding watermark: %+v", tableWatermark.LowWatermark())
				}

				sc.logger.Infof(
					"Resuming snapshotting of table '%s' at <<%s>> up to <<%s>>",
					table.CanonicalName(), lowWatermarkComparison, comparison,
				)

				comparison = fmt.Sprintf("%s AND %s",
					lowWatermarkComparison,
					comparison,
				)
			} else {
				sc.logger.Infof(
					"Starting snapshotting of table '%s' up to <<%s>>",
					table.CanonicalName(), comparison,
				)
			}

			cursorName := lo.RandomString(15, lo.LowerCaseLettersCharset)
			cursorQuery := fmt.Sprintf(
				`DECLARE %s SCROLL CURSOR FOR SELECT * FROM %s WHERE %s ORDER BY %s LIMIT %d`,
				cursorName, table.CanonicalName(), comparison,
				index.AsSqlOrderBy(false), snapshotBatchSize*10,
			)

			hook := func(lsn pgtypes.LSN, values map[string]any) error {
				indexValues := lo.PickBy(values, func(key string, _ any) bool {
					for _, column := range index.Columns() {
						if column.Name() == key {
							return true
						}
					}
					return false
				})

				tableWatermark.SetLowWatermark(indexValues)
				return cb(lsn, values)
			}

			return sc.snapshotTableWithCursor(
				rowDecoderFactory, cursorQuery, cursorName, &snapshotName, snapshotBatchSize, hook,
			)
		},
	)
}

func (sc *sideChannel) ReadSnapshotHighWatermark(
	rowDecoderFactory pgtypes.RowDecoderFactory, table systemcatalog.BaseTable, snapshotName string,
) (values map[string]any, err error) {

	index, present := table.Columns().SnapshotIndex()
	if !present {
		return nil, errors.Errorf(
			"missing snapshotable index for table '%s'", table.CanonicalName(),
		)
	}

	query := fmt.Sprintf(
		queryTemplateSnapshotHighWatermark, index.AsSqlTuple(),
		table.CanonicalName(), index.AsSqlOrderBy(true),
	)
	if err := sc.newSession(time.Second*10, func(session *session) error {
		if _, err := session.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return errors.Wrap(err, 0)
		}

		if _, err := session.exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
			return errors.Wrap(err, 0)
		}

		return session.queryFunc(func(row pgx.Row) error {
			rows := row.(pgx.Rows)

			rowDecoder, err := rowDecoderFactory(rows.FieldDescriptions())
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

func (sc *sideChannel) ReadReplicaIdentity(
	schemaName, tableName string,
) (pgtypes.ReplicaIdentity, error) {

	var replicaIdentity pgtypes.ReplicaIdentity
	if err := sc.newSession(time.Second*10, func(session *session) error {
		row := session.queryRow(queryReadReplicaIdentity, schemaName, tableName)

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

func (sc *sideChannel) ReadContinuousAggregate(
	materializedHypertableId int32,
) (viewSchema, viewName string, found bool, err error) {

	if err := sc.newSession(time.Second*10, func(session *session) error {
		row := session.queryRow(queryReadContinuousAggregateInformation, materializedHypertableId)
		if err := row.Scan(&viewSchema, &viewName); err != nil {
			if err != pgx.ErrNoRows {
				return errors.Wrap(err, 0)
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

func (sc *sideChannel) ReadPublishedTables(
	publicationName string,
) ([]systemcatalog.SystemEntity, error) {

	systemEntities := make([]systemcatalog.SystemEntity, 0)
	if err := sc.newSession(time.Second*20, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			var schemaName, tableName string
			if err := row.Scan(&schemaName, &tableName); err != nil {
				return errors.Wrap(err, 0)
			}
			systemEntities = append(systemEntities, systemcatalog.NewSystemEntity(schemaName, tableName))
			return nil
		}, queryReadExistingAlreadyPublishedTables, publicationName)
	}); err != nil {
		return nil, err
	}
	return systemEntities, nil
}

func (sc *sideChannel) ReadReplicationSlot(
	slotName string,
) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		var restart, confirmed string
		if err := session.queryRow(queryReadReplicationSlot, slotName).Scan(
			&pluginName, &slotType, &restart, &confirmed,
		); err != nil {
			return err
		}
		lsn, err := pglogrepl.ParseLSN(restart)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		restartLsn = pgtypes.LSN(lsn)
		lsn, err = pglogrepl.ParseLSN(confirmed)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		confirmedFlushLsn = pgtypes.LSN(lsn)
		return nil
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	if err != nil {
		err = errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) ExistsReplicationSlot(
	slotName string,
) (found bool, err error) {

	err = sc.newSession(time.Second*10, func(session *session) error {
		return session.queryRow(queryCheckReplicationSlotExists, slotName).Scan(&found)
	})
	if err == pgx.ErrNoRows {
		err = nil
	}
	if err != nil {
		err = errors.Wrap(err, 0)
	}
	return
}

func (sc *sideChannel) ReadPgCompositeTypeSchema(
	oid uint32, compositeColumnFactory pgtypes.CompositeColumnFactory,
) ([]pgtypes.CompositeColumn, error) {

	columns := make([]pgtypes.CompositeColumn, 0)
	if err := sc.newSession(time.Second*10, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			var attname string
			var atttypeid uint32
			var atttypmod int
			var attnotnull bool

			if err := row.Scan(&attname, &atttypeid, &atttypmod, &attnotnull); err != nil {
				return err
			}

			column, err := compositeColumnFactory(attname, atttypeid, atttypmod, !attnotnull)
			if err != nil {
				return err
			}

			columns = append(columns, column)
			return nil
		}, queryReadCompositeTypeSchema, oid)
	}); err != nil {
		return nil, err
	}

	return columns, nil
}

func (sc *sideChannel) ReadPgTypes(
	factory pgtypes.TypeFactory, cb func(pgType pgtypes.PgType) error, oids ...uint32,
) error {

	query := fmt.Sprintf(queryTemplateReadPostgreSqlTypes, "")
	arguments := make([]any, 0)
	if len(oids) == 1 {
		query = fmt.Sprintf(queryTemplateReadPostgreSqlTypes, "AND t.oid = $1")
		arguments = append(arguments, oids[0])
	} else if len(oids) > 1 {
		query = fmt.Sprintf(queryTemplateReadPostgreSqlTypes, "AND t.oid = ANY($1)")
		arguments = append(arguments, oids)
	}

	return sc.newSession(time.Second*30, func(session *session) error {
		return session.queryFunc(func(row pgx.Row) error {
			typ, found, err := sc.scanPgType(row, factory)
			if err != nil {
				return err
			}

			if !found {
				return nil
			}

			// Record types aren't supported at the moment, so we can simply skip them
			if typ.IsRecord() && typ.Kind() != pgtypes.CompositeKind {
				return nil
			}

			return cb(typ)
		}, query, arguments...)
	})
}

func (sc *sideChannel) scanPgType(
	row pgx.Row, factory pgtypes.TypeFactory,
) (pgtypes.PgType, bool, error) {

	var namespace, name string
	var kind, category, delimiter int32
	var arrayType, recordType bool
	var oid, oidArray, oidElement, baseOid uint32
	var modifiers int
	var enumValues []string

	if err := row.Scan(
		&namespace, &name, &arrayType, &recordType, &kind, &oid, &oidArray, &oidElement,
		&category, &baseOid, &modifiers, &enumValues, &delimiter,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, errors.Wrap(err, 0)
	}

	return factory(namespace, name, pgtypes.PgKind(kind), oid, pgtypes.PgCategory(category),
		arrayType, recordType, oidArray, oidElement, baseOid, modifiers, enumValues, string(delimiter),
	), true, nil
}

func (sc *sideChannel) entitiesToTableList(
	entities []systemcatalog.SystemEntity,
) string {

	if len(entities) == 1 {
		return entities[0].CanonicalName()
	}

	canonicalEntityNames := make([]string, len(entities))
	for i, entity := range entities {
		canonicalEntityNames[i] = entity.CanonicalName()
	}

	return strings.Join(canonicalEntityNames, ",")
}

func (sc *sideChannel) readVanillaTableSchema0(
	session *session, table *systemcatalog.PgTable,
	pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
	cb sidechannel.TableSchemaCallback,
) error {

	columns := make([]systemcatalog.Column, 0)
	if err := session.queryFunc(func(row pgx.Row) error {
		var name, sortOrder, nullsOrder string
		var oid uint32
		var modifiers int
		var keySeq, maxCharLength *int
		var nullable, primaryKey, isReplicaIdent bool
		var defaultValue, indexName *string

		if err := row.Scan(&name, &oid, &modifiers, &nullable, &primaryKey, &keySeq, &defaultValue,
			&isReplicaIdent, &indexName, &sortOrder, &nullsOrder, &maxCharLength); err != nil {

			return errors.Wrap(err, 0)
		}

		dataType, err := pgTypeResolver(oid)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		column := systemcatalog.NewIndexColumn(
			name, oid, modifiers, dataType, nullable, primaryKey, keySeq, defaultValue, isReplicaIdent, indexName,
			systemcatalog.IndexSortOrder(sortOrder), systemcatalog.IndexNullsOrder(nullsOrder),
			false, false, nil, nil, maxCharLength,
		)
		columns = append(columns, column)
		return nil
	}, queryReadVanillaTableSchema, table.SchemaName(), table.TableName()); err != nil {
		return err
	}

	if err := cb(table, columns); err != nil {
		return err
	}

	return nil
}

func (sc *sideChannel) readHypertableSchema0(
	session *session, hypertable *systemcatalog.Hypertable,
	pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
	cb sidechannel.TableSchemaCallback,
) error {

	columns := make([]systemcatalog.Column, 0)
	if err := session.queryFunc(func(row pgx.Row) error {
		var name, sortOrder, nullsOrder string
		var oid uint32
		var modifiers int
		var keySeq, dimSeq, maxCharLength *int
		var nullable, primaryKey, isReplicaIdent, dimension, dimAligned bool
		var defaultValue, indexName, dimType *string

		if err := row.Scan(&name, &oid, &modifiers, &nullable, &primaryKey, &keySeq,
			&defaultValue, &isReplicaIdent, &indexName, &sortOrder, &nullsOrder,
			&dimension, &dimAligned, &dimType, &dimSeq, &maxCharLength); err != nil {

			return errors.Wrap(err, 0)
		}

		dataType, err := pgTypeResolver(oid)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		column := systemcatalog.NewIndexColumn(
			name, oid, modifiers, dataType, nullable, primaryKey, keySeq, defaultValue, isReplicaIdent, indexName,
			systemcatalog.IndexSortOrder(sortOrder), systemcatalog.IndexNullsOrder(nullsOrder),
			dimension, dimAligned, dimType, dimSeq, maxCharLength,
		)
		columns = append(columns, column)
		return nil
	}, queryReadHypertableSchema, hypertable.SchemaName(), hypertable.TableName()); err != nil {
		return err
	}

	if err := cb(hypertable, columns); err != nil {
		return err
	}

	return nil
}

func (sc *sideChannel) snapshotTableWithCursor(
	rowDecoderFactory pgtypes.RowDecoderFactory, cursorQuery, cursorName string,
	snapshotName *string, snapshotBatchSize int, cb sidechannel.SnapshotRowCallback,
) error {

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

		var rowDecoder pgtypes.RowDecoder
		for {
			count := 0
			if err := session.queryFunc(func(row pgx.Row) error {
				rows := row.(pgx.Rows)

				if rowDecoder == nil {
					// Initialize the row decoder based on the returned field descriptions
					rd, err := rowDecoderFactory(rows.FieldDescriptions())
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

func (sc *sideChannel) newSession(
	timeout time.Duration, fn func(session *session) error,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	connection, err := sc.newSideChannelConnection(ctx)
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

func (sc *sideChannel) newSideChannelConnection(
	ctx context.Context,
) (*pgx.Conn, error) {

	return pgx.ConnectConfig(ctx, sc.pgxConfig)
}

type rowFunction = func(
	row pgx.Row,
) error

type session struct {
	connection *pgx.Conn
	ctx        context.Context
	cancel     func()
}

func (s *session) ResetTimeout(
	timeout time.Duration,
) {

	// Cancel old context
	s.cancel()

	// Initialize new timeout
	s.ctx, s.cancel = context.WithTimeout(context.Background(), timeout)
}

func (s *session) queryFunc(
	fn rowFunction, query string, args ...any,
) error {

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

func (s *session) queryRow(
	query string, args ...any,
) pgx.Row {

	return s.connection.QueryRow(s.ctx, query, args...)
}

func (s *session) exec(
	query string, args ...any,
) (pgconn.CommandTag, error) {

	return s.connection.Exec(s.ctx, query, args...)
}
