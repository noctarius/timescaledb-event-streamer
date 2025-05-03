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
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
)

var ErrNoRestartPointInReplicationSlot = errors.New("Restart LSN in replication slot is unavailable")

type TableGrant string

const (
	Select     TableGrant = "select"
	Insert     TableGrant = "insert"
	Update     TableGrant = "update"
	Delete     TableGrant = "delete"
	Truncate   TableGrant = "truncate"
	References TableGrant = "references"
	Trigger    TableGrant = "trigger"
)

type TableSchemaCallback = func(
	table systemcatalog.SystemEntity, columns []systemcatalog.Column,
) error

type SnapshotRowCallback = func(
	lsn pgtypes.LSN, values map[string]any,
) error

type SideChannel interface {
	HasTablePrivilege(
		username string, entity systemcatalog.SystemEntity, grant TableGrant,
	) (access bool, err error)
	CreatePublication(
		publicationName string,
	) (success bool, err error)
	ExistsPublication(
		publicationName string,
	) (found bool, err error)
	DropPublication(
		publicationName string,
	) error
	ExistsTableInPublication(
		publicationName, schemaName, tableName string,
	) (found bool, err error)
	GetSystemInformation() (
		databaseName, systemId string, timeline int32, err error,
	)
	GetWalLevel() (walLevel string, err error)
	GetReplicationMarkersEnabled() (enabled bool, err error)
	GetPostgresVersion() (pgVersion version.PostgresVersion, err error)
	GetTimescaleDBVersion() (tsdbVersion version.TimescaleVersion, found bool, err error)
	ReadHypertables(
		cb func(hypertable *systemcatalog.Hypertable) error,
	) error
	ReadChunks(
		cb func(chunk *systemcatalog.Chunk) error,
	) error
	ReadVanillaTables(
		cb func(table *systemcatalog.PgTable) error,
	) error
	ReadHypertableSchema(
		cb TableSchemaCallback,
		pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
		hypertables ...*systemcatalog.Hypertable,
	) error
	ReadVanillaTableSchema(
		cb TableSchemaCallback,
		pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
		tables ...*systemcatalog.PgTable,
	) error
	AttachTablesToPublication(
		publicationName string, entities ...systemcatalog.SystemEntity,
	) error
	DetachTablesFromPublication(
		publicationName string, entities ...systemcatalog.SystemEntity,
	) error
	SnapshotChunkTable(
		rowDecoderFactory pgtypes.RowDecoderFactory, chunk *systemcatalog.Chunk,
		snapshotBatchSize int, cb SnapshotRowCallback,
	) (lsn pgtypes.LSN, err error)
	FetchTableSnapshotBatch(
		rowDecoderFactory pgtypes.RowDecoderFactory, table systemcatalog.BaseTable,
		snapshotName string, snapshotBatchSize int, cb SnapshotRowCallback,
	) error
	ReadSnapshotHighWatermark(
		rowDecoderFactory pgtypes.RowDecoderFactory, table systemcatalog.BaseTable, snapshotName string,
	) (values map[string]any, err error)
	ReadReplicaIdentity(
		schemaName, tableName string,
	) (identity pgtypes.ReplicaIdentity, err error)
	ReadContinuousAggregate(
		materializedHypertableId int32,
	) (viewSchema, viewName string, found bool, err error)
	ReadPublishedTables(
		publicationName string,
	) (entities []systemcatalog.SystemEntity, err error)
	ReadReplicationSlot(
		slotName string,
	) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error)
	ExistsReplicationSlot(
		slotName string,
	) (found bool, err error)
	ReadPgTypes(
		factory pgtypes.TypeFactory, cb func(typ pgtypes.PgType) error, oids ...uint32,
	) error
	ReadPgCompositeTypeSchema(
		oid uint32, compositeColumnFactory pgtypes.CompositeColumnFactory,
	) ([]pgtypes.CompositeColumn, error)
}
