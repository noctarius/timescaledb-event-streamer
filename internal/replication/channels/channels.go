package channels

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventhandler"
	"github.com/noctarius/timescaledb-event-streamer/internal/pg"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/model"
)

type SideChannel interface {
	ReadHypertables(cb func(hypertable *model.Hypertable) error) error

	ReadChunks(cb func(chunk *model.Chunk) error) error

	ReadHypertableSchema(cb func(hypertable *model.Hypertable, columns []model.Column) bool,
		hypertables ...*model.Hypertable) error

	AttachChunkToPublication(chunk *model.Chunk) error

	DetachChunkFromPublication(chunk *model.Chunk) error

	SnapshotTable(canonicalName string, startingLSN *pglogrepl.LSN,
		cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error)

	ReadReplicaIdentity(schemaName, tableName string) (pg.ReplicaIdentity, error)

	ReadContinuousAggregate(materializedHypertableId int32) (viewSchema, viewName string, found bool, err error)

	ReadPublishedTables(publicationName string) ([]string, error)

	GetPostgresVersion() (version uint, err error)

	GetTimescaleDBVersion() (version uint, err error)
}

type ReplicationChannel interface {
	StartReplicationChannel(dispatcher *eventhandler.Dispatcher, initialChunkTables []string) error

	StopReplicationChannel() error
}
