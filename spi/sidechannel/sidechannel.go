package sidechannel

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
)

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

type HypertableSchemaCallback = func(
	hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column,
) bool

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
	GetPostgresVersion() (pgVersion version.PostgresVersion, err error)
	GetTimescaleDBVersion() (tsdbVersion version.TimescaleVersion, found bool, err error)
	ReadHypertables(
		cb func(hypertable *systemcatalog.Hypertable) error,
	) error
	ReadChunks(
		cb func(chunk *systemcatalog.Chunk) error,
	) error
	ReadHypertableSchema(
		cb HypertableSchemaCallback,
		pgTypeResolver func(oid uint32) (pgtypes.PgType, error),
		hypertables ...*systemcatalog.Hypertable,
	) error
	AttachTablesToPublication(
		publicationName string, entities ...systemcatalog.SystemEntity,
	) error
	DetachTablesFromPublication(
		publicationName string, entities ...systemcatalog.SystemEntity,
	) error
	SnapshotChunkTable(
		chunk *systemcatalog.Chunk, snapshotBatchSize int, cb SnapshotRowCallback,
	) (lsn pgtypes.LSN, err error)
	FetchHypertableSnapshotBatch(
		hypertable *systemcatalog.Hypertable, snapshotName string, snapshotBatchSize int, cb SnapshotRowCallback,
	) error
	ReadSnapshotHighWatermark(
		hypertable *systemcatalog.Hypertable, snapshotName string,
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
		factory pgtypes.TypeFactory, cb func(pgType pgtypes.PgType) error, oids ...uint32,
	) error
}
