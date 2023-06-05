package eventhandlers

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type BaseReplicationEventHandler interface {
	OnRelationEvent(xld pgtypes.XLogData, msg *pgtypes.RelationMessage) error
}

type LogicalReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnBeginEvent(xld pgtypes.XLogData, msg *pgtypes.BeginMessage) error
	OnCommitEvent(xld pgtypes.XLogData, msg *pgtypes.CommitMessage) error
	OnInsertEvent(xld pgtypes.XLogData, msg *pgtypes.InsertMessage) error
	OnUpdateEvent(xld pgtypes.XLogData, msg *pgtypes.UpdateMessage) error
	OnDeleteEvent(xld pgtypes.XLogData, msg *pgtypes.DeleteMessage) error
	OnTruncateEvent(xld pgtypes.XLogData, msg *pgtypes.TruncateMessage) error
	OnTypeEvent(xld pgtypes.XLogData, msg *pgtypes.TypeMessage) error
	OnOriginEvent(xld pgtypes.XLogData, msg *pgtypes.OriginMessage) error
	OnMessageEvent(xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage) error
}

type HypertableReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnReadEvent(lsn pgtypes.LSN, hypertable *systemcatalog.Hypertable,
		chunk *systemcatalog.Chunk, newValues map[string]any) error
	OnBeginEvent(xld pgtypes.XLogData, msg *pgtypes.BeginMessage) error
	OnCommitEvent(xld pgtypes.XLogData, msg *pgtypes.CommitMessage) error
	OnInsertEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
		newValues map[string]any) error
	OnUpdateEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
		oldValues, newValues map[string]any) error
	OnDeleteEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
		oldValues map[string]any, tombstone bool) error
	OnTruncateEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable) error
	OnTypeEvent(xld pgtypes.XLogData, msg *pgtypes.TypeMessage) error
	OnOriginEvent(xld pgtypes.XLogData, msg *pgtypes.OriginMessage) error
	OnMessageEvent(xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage) error
}

type ChunkSnapshotEventHandler interface {
	BaseReplicationEventHandler
	OnChunkSnapshotStartedEvent(hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk) error
	OnChunkSnapshotFinishedEvent(hypertable *systemcatalog.Hypertable,
		chunk *systemcatalog.Chunk, snapshot pgtypes.LSN) error
}

type CompressionReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnChunkCompressedEvent(xld pgtypes.XLogData,
		hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk) error
	OnChunkDecompressedEvent(xld pgtypes.XLogData,
		hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk) error
}

type SystemCatalogReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnHypertableAddedEvent(xld pgtypes.XLogData, relationId uint32, newValues map[string]any) error
	OnHypertableUpdatedEvent(xld pgtypes.XLogData, relationId uint32, oldValues, newValues map[string]any) error
	OnHypertableDeletedEvent(xld pgtypes.XLogData, relationId uint32, oldValues map[string]any) error
	OnChunkAddedEvent(xld pgtypes.XLogData, relationId uint32, newValues map[string]any) error
	OnChunkUpdatedEvent(xld pgtypes.XLogData, relationId uint32, oldValues, newValues map[string]any) error
	OnChunkDeletedEvent(xld pgtypes.XLogData, relationId uint32, oldValues map[string]any) error
}
