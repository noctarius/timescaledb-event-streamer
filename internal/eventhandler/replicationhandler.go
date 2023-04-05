package eventhandler

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
)

type BaseReplicationEventHandler interface {
	OnRelationEvent(xld pglogrepl.XLogData, msg *pglogrepl.RelationMessage) error
}

type LogicalReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnBeginEvent(xld pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error
	OnCommitEvent(xld pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error
	OnInsertEvent(xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage, newValues map[string]any) error
	OnUpdateEvent(xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage, oldValues, newValues map[string]any) error
	OnDeleteEvent(xld pglogrepl.XLogData, msg *pglogrepl.DeleteMessage, oldValues map[string]any) error
	OnTruncateEvent(xld pglogrepl.XLogData, msg *pglogrepl.TruncateMessage) error
	OnTypeEvent(xld pglogrepl.XLogData, msg *pglogrepl.TypeMessage) error
	OnOriginEvent(xld pglogrepl.XLogData, msg *pglogrepl.OriginMessage) error
	OnMessageEvent(xld pglogrepl.XLogData, msg *decoding.LogicalReplicationMessage) error
}

type HypertableReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnReadEvent(lsn pglogrepl.LSN, hypertable *model.Hypertable, chunk *model.Chunk, newValues map[string]any) error
	OnBeginEvent(xld pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error
	OnCommitEvent(xld pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error
	OnInsertEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk,
		newValues map[string]any) error
	OnUpdateEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk,
		oldValues, newValues map[string]any) error
	OnDeleteEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk,
		oldValues map[string]any, tombstone bool) error
	OnTruncateEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable) error
	OnTypeEvent(xld pglogrepl.XLogData, msg *pglogrepl.TypeMessage) error
	OnOriginEvent(xld pglogrepl.XLogData, msg *pglogrepl.OriginMessage) error
	OnMessageEvent(xld pglogrepl.XLogData, msg *decoding.LogicalReplicationMessage) error
}

type ChunkSnapshotEventHandler interface {
	BaseReplicationEventHandler
	OnChunkSnapshotStartedEvent(hypertable *model.Hypertable, chunk *model.Chunk) error
	OnChunkSnapshotFinishedEvent(hypertable *model.Hypertable, chunk *model.Chunk, snapshot pglogrepl.LSN) error
}

type CompressionReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnChunkCompressedEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk) error
	OnChunkDecompressedEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk) error
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
