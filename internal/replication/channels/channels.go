package channels

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventhandler"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
)

type SideChannel interface {
	ReadHypertables(cb func(hypertable *systemcatalog.Hypertable) error) error

	ReadChunks(cb func(chunk *systemcatalog.Chunk) error) error

	ReadHypertableSchema(cb func(hypertable *systemcatalog.Hypertable,
		columns []systemcatalog.Column) bool, hypertables ...*systemcatalog.Hypertable) error

	AttachChunkToPublication(chunk *systemcatalog.Chunk) error

	DetachChunkFromPublication(chunk *systemcatalog.Chunk) error

	SnapshotTable(canonicalName string, startingLSN *pglogrepl.LSN,
		cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error)

	ReadReplicaIdentity(schemaName, tableName string) (pgtypes.ReplicaIdentity, error)

	ReadContinuousAggregate(materializedHypertableId int32) (viewSchema, viewName string, found bool, err error)

	ReadPublishedTables(publicationName string) ([]string, error)

	GetPostgresVersion() (version version.PostgresVersion, err error)

	GetTimescaleDBVersion() (version version.TimescaleVersion, err error)

	GetWalLevel() (walLevel string, err error)
}

type ReplicationChannel interface {
	StartReplicationChannel(dispatcher *eventhandler.Dispatcher, initialChunkTables []string) error

	StopReplicationChannel() error
}
