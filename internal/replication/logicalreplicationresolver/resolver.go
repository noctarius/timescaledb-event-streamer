package logicalreplicationresolver

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
)

var logger = logging.NewLogger("LogicalReplicationResolver")

type LogicalReplicationResolver struct {
	dispatcher    *eventhandler.Dispatcher
	systemCatalog *systemcatalog.SystemCatalog
	relations     map[uint32]*pglogrepl.RelationMessage
	eventQueues   map[string]*replicationQueue

	genReadEvent          bool
	genInsertEvent        bool
	genUpdateEvent        bool
	genDeleteEvent        bool
	genTruncateEvent      bool
	genCompressionEvent   bool
	genDecompressionEvent bool
}

func NewLogicalReplicationResolver(config *configuring.Config, dispatcher *eventhandler.Dispatcher,
	systemCatalog *systemcatalog.SystemCatalog) *LogicalReplicationResolver {

	return &LogicalReplicationResolver{
		dispatcher:    dispatcher,
		systemCatalog: systemCatalog,
		relations:     make(map[uint32]*pglogrepl.RelationMessage, 0),
		eventQueues:   make(map[string]*replicationQueue, 0),

		genReadEvent:          configuring.GetOrDefault(config, "timescaledb.events.read", true),
		genInsertEvent:        configuring.GetOrDefault(config, "timescaledb.events.insert", true),
		genUpdateEvent:        configuring.GetOrDefault(config, "timescaledb.events.update", true),
		genDeleteEvent:        configuring.GetOrDefault(config, "timescaledb.events.delete", true),
		genTruncateEvent:      configuring.GetOrDefault(config, "timescaledb.events.truncate", true),
		genCompressionEvent:   configuring.GetOrDefault(config, "timescaledb.events.compression", true),
		genDecompressionEvent: configuring.GetOrDefault(config, "timescaledb.events.decompression", true),
	}
}

func (l *LogicalReplicationResolver) OnChunkSnapshotStartedEvent(_ *model.Hypertable, chunk *model.Chunk) error {
	l.eventQueues[chunk.CanonicalName()] = newReplicationQueue()
	logger.Printf("Snapshot of %s started", chunk.CanonicalName())
	return nil
}

func (l *LogicalReplicationResolver) OnChunkSnapshotFinishedEvent(
	_ *model.Hypertable, chunk *model.Chunk, snapshot pglogrepl.LSN) error {

	queue := l.eventQueues[chunk.CanonicalName()]
	for {
		fn := queue.pop()
		// initial queue empty, remove it now, to prevent additional messages
		// being enqueued.
		if fn == nil {
			delete(l.eventQueues, chunk.CanonicalName())
			queue.lock()
			break
		}

		if err := fn(snapshot); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// second round to make sure there wasn't any concurrent writes
	for {
		fn := queue.pop()
		if fn == nil {
			break
		}

		if err := fn(snapshot); err != nil {
			return errors.Wrap(err, 0)
		}
	}
	logger.Printf("Snapshot of %s finished", chunk.CanonicalName())

	return nil
}

func (l *LogicalReplicationResolver) OnRelationEvent(xld pglogrepl.XLogData, msg *pglogrepl.RelationMessage) error {
	logger.Printf("RELATION MSG: %+v", msg)
	l.relations[msg.RelationID] = msg
	return nil
}

func (l *LogicalReplicationResolver) OnBeginEvent(xld pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error {
	logger.Printf("BEGIN MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *LogicalReplicationResolver) OnCommitEvent(xld pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error {
	logger.Printf("COMMIT MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *LogicalReplicationResolver) OnInsertEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if l.isHypertableEvent(rel) {
		return l.onHypertableInsertEvent(msg, newValues)
	}

	if l.isChunkEvent(rel) {
		return l.onChunkInsertEvent(msg, newValues)
	}

	chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
	if hypertable != nil {

		if !l.genInsertEvent {
			return nil
		}

		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnInsertEvent(xld, hypertable, chunk, newValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *LogicalReplicationResolver) OnUpdateEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage, oldValues, newValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if l.isHypertableEvent(rel) {
		return l.onHypertableUpdateEvent(msg, oldValues, newValues)
	}

	if l.isChunkEvent(rel) {
		chunkId := newValues["id"].(int32)
		if chunk := l.systemCatalog.FindChunkById(chunkId); chunk != nil {
			hypertable := l.systemCatalog.FindHypertableById(chunk.HypertableId())
			if chunk.Status() == 0 && (newValues["status"].(int32)) == 1 {
				return l.onChunkCompressionEvent(xld, hypertable, rel, chunk)
			}
		}

		return l.onChunkUpdateEvent(msg, oldValues, newValues)
	}

	chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
	if !l.genUpdateEvent {
		return nil
	}

	if hypertable != nil {
		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnUpdateEvent(xld, hypertable, chunk, oldValues, newValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *LogicalReplicationResolver) OnDeleteEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if l.isHypertableEvent(rel) {
		return l.onHypertableDeleteEvent(msg, oldValues)
	}

	if l.isChunkEvent(rel) {
		return l.onChunkDeleteEvent(xld, msg, oldValues)
	}

	if !l.genUpdateEvent {
		return nil
	}

	chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
	if hypertable != nil {
		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnDeleteEvent(xld, hypertable, chunk, oldValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *LogicalReplicationResolver) OnTruncateEvent(xld pglogrepl.XLogData, msg *pglogrepl.TruncateMessage) error {
	if !l.genTruncateEvent {
		return nil
	}

	truncatedHypertables := make([]*model.Hypertable, 0)
	for i := 0; i < int(msg.RelationNum); i++ {
		rel, ok := l.relations[msg.RelationIDs[i]]
		if !ok {
			logger.Fatalf("unknown relation ID %d", msg.RelationIDs[i])
		}

		if l.isHypertableEvent(rel) || l.isChunkEvent(rel) {
			// Catalog tables shouldn't be truncated; EVER!
			continue
		}

		_, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
		truncatedHypertables = append(truncatedHypertables, hypertable)
	}

	truncatedHypertables = supporting.DistinctItems(truncatedHypertables, func(item *model.Hypertable) string {
		return item.CanonicalName()
	})

	for _, hypertable := range truncatedHypertables {
		if err := l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyHypertableReplicationEventHandler(
				func(handler eventhandler.HypertableReplicationEventHandler) error {
					return handler.OnTruncateEvent(xld, hypertable)
				},
			)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *LogicalReplicationResolver) OnTypeEvent(xld pglogrepl.XLogData, msg *pglogrepl.TypeMessage) error {
	logger.Printf("TYPE MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *LogicalReplicationResolver) OnOriginEvent(xld pglogrepl.XLogData, msg *pglogrepl.OriginMessage) error {
	logger.Printf("ORIGIN MSG: %+v", msg)
	//TODO implement me
	logger.Printf("Origin: %+v", msg)
	return nil
}

func (l *LogicalReplicationResolver) onHypertableInsertEvent(
	msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableAddedEvent(msg.RelationID, newValues)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onChunkInsertEvent(
	msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkAddedEvent(msg.RelationID, newValues)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onHypertableUpdateEvent(msg *pglogrepl.UpdateMessage,
	oldValues map[string]any, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableUpdatedEvent(msg.RelationID, oldValues, newValues)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onChunkUpdateEvent(msg *pglogrepl.UpdateMessage,
	oldValues map[string]any, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkUpdatedEvent(msg.RelationID, oldValues, newValues)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onHypertableDeleteEvent(
	msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableDeletedEvent(msg.RelationID, oldValues)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onChunkDeleteEvent(xld pglogrepl.XLogData,
	msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	if id, ok := oldValues["id"].(int32); ok {
		chunk := l.systemCatalog.FindChunkById(id)
		if chunk.IsCompressed() {
			if err := l.onChunkDecompressionEvent(xld, chunk); err != nil {
				return err
			}
		}
	}

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkDeletedEvent(msg.RelationID, oldValues)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onChunkCompressionEvent(xld pglogrepl.XLogData,
	hypertable *model.Hypertable, rel *pglogrepl.RelationMessage, chunk *model.Chunk) error {

	if !l.genCompressionEvent {
		return nil
	}

	//uncompressedHypertable := l.systemCatalog.FindHypertableByCompressedHypertableId(hypertable.Id())
	logger.Printf(
		"COMPRESSION EVENT %s.%s FOR CHUNK %s.%s", hypertable.SchemaName(),
		hypertable.HypertableName(), rel.Namespace, rel.RelationName,
	)

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyCompressionReplicationEventHandler(
			func(handler eventhandler.CompressionReplicationEventHandler) error {
				return handler.OnChunkCompressedEvent(xld, hypertable, chunk)
			},
		)
	})
}

func (l *LogicalReplicationResolver) onChunkDecompressionEvent(xld pglogrepl.XLogData, chunk *model.Chunk) error {
	hypertableId := chunk.HypertableId()
	uncompressedHypertable := l.systemCatalog.FindHypertableByCompressedHypertableId(hypertableId)
	logger.Printf(
		"DECOMPRESSION EVENT %s.%s FOR CHUNK %s.%s", uncompressedHypertable.SchemaName(),
		uncompressedHypertable.HypertableName(), chunk.SchemaName(), chunk.TableName(),
	)

	if !l.genDecompressionEvent {
		return nil
	}

	if err := l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyCompressionReplicationEventHandler(
			func(handler eventhandler.CompressionReplicationEventHandler) error {
				return handler.OnChunkDecompressedEvent(xld, uncompressedHypertable, chunk)
			},
		)
	}); err != nil {
		return err
	}
	return nil
}

func (l *LogicalReplicationResolver) isHypertableEvent(relation *pglogrepl.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "hypertable"
}

func (l *LogicalReplicationResolver) isChunkEvent(relation *pglogrepl.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "chunk"
}

func (l *LogicalReplicationResolver) isHypertableChunkEvent(relation *pglogrepl.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_internal"
}

func (l *LogicalReplicationResolver) enqueueOrExecute(chunk *model.Chunk,
	xld pglogrepl.XLogData, fn func() error) error {

	if l.isSnapshotting(chunk) {
		queue := l.eventQueues[chunk.CanonicalName()]
		if ok := queue.push(func(snapshot pglogrepl.LSN) error {
			if xld.ServerWALEnd < snapshot {
				return nil
			}
			return fn()
		}); ok {
			return nil
		}
	}

	return fn()
}

func (l *LogicalReplicationResolver) isSnapshotting(chunk *model.Chunk) bool {
	_, present := l.eventQueues[chunk.CanonicalName()]
	return present
}

func (l *LogicalReplicationResolver) resolveChunkAndHypertable(
	schemaName, tableName string) (*model.Chunk, *model.Hypertable) {

	chunk := l.systemCatalog.FindChunkByName(schemaName, tableName)
	if chunk == nil {
		return nil, nil
	}

	hypertable := l.systemCatalog.FindHypertableById(chunk.HypertableId())
	return chunk, hypertable
}
