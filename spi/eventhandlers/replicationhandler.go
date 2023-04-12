package eventhandlers

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type BaseReplicationEventHandler interface {
	OnRelationEvent(xld pglogrepl.XLogData, msg *pgtypes.RelationMessage) error
}

type LogicalReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnBeginEvent(xld pglogrepl.XLogData, msg *pgtypes.BeginMessage) error
	OnCommitEvent(xld pglogrepl.XLogData, msg *pgtypes.CommitMessage) error
	OnInsertEvent(xld pglogrepl.XLogData, msg *pgtypes.InsertMessage) error
	OnUpdateEvent(xld pglogrepl.XLogData, msg *pgtypes.UpdateMessage) error
	OnDeleteEvent(xld pglogrepl.XLogData, msg *pgtypes.DeleteMessage) error
	OnTruncateEvent(xld pglogrepl.XLogData, msg *pgtypes.TruncateMessage) error
	OnTypeEvent(xld pglogrepl.XLogData, msg *pgtypes.TypeMessage) error
	OnOriginEvent(xld pglogrepl.XLogData, msg *pgtypes.OriginMessage) error
	OnMessageEvent(xld pglogrepl.XLogData, msg *pgtypes.LogicalReplicationMessage) error
}

type HypertableReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnReadEvent(lsn pglogrepl.LSN, hypertable *systemcatalog.Hypertable,
		chunk *systemcatalog.Chunk, newValues map[string]any) error
	OnBeginEvent(xld pglogrepl.XLogData, msg *pgtypes.BeginMessage) error
	OnCommitEvent(xld pglogrepl.XLogData, msg *pgtypes.CommitMessage) error
	OnInsertEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
		newValues map[string]any) error
	OnUpdateEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
		oldValues, newValues map[string]any) error
	OnDeleteEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
		oldValues map[string]any, tombstone bool) error
	OnTruncateEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable) error
	OnTypeEvent(xld pglogrepl.XLogData, msg *pgtypes.TypeMessage) error
	OnOriginEvent(xld pglogrepl.XLogData, msg *pgtypes.OriginMessage) error
	OnMessageEvent(xld pglogrepl.XLogData, msg *pgtypes.LogicalReplicationMessage) error
}

type ChunkSnapshotEventHandler interface {
	BaseReplicationEventHandler
	OnChunkSnapshotStartedEvent(hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk) error
	OnChunkSnapshotFinishedEvent(hypertable *systemcatalog.Hypertable,
		chunk *systemcatalog.Chunk, snapshot pglogrepl.LSN) error
}

type CompressionReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnChunkCompressedEvent(xld pglogrepl.XLogData,
		hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk) error
	OnChunkDecompressedEvent(xld pglogrepl.XLogData,
		hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk) error
}

type SystemCatalogReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnHypertableAddedEvent(relationId uint32, newValues map[string]any) error
	OnHypertableUpdatedEvent(relationId uint32, oldValues, newValues map[string]any) error
	OnHypertableDeletedEvent(relationId uint32, oldValues map[string]any) error
	OnChunkAddedEvent(relationId uint32, newValues map[string]any) error
	OnChunkUpdatedEvent(relationId uint32, oldValues, newValues map[string]any) error
	OnChunkDeletedEvent(relationId uint32, oldValues map[string]any) error
}
