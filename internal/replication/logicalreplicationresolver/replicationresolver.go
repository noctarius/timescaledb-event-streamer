package logicalreplicationresolver

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
)

var logger = logging.NewLogger("logicalReplicationResolver")

type logicalReplicationResolver struct {
	dispatcher    *eventhandler.Dispatcher
	systemCatalog *systemcatalog.SystemCatalog
	relations     map[uint32]*pglogrepl.RelationMessage
	eventQueues   map[string]*replicationQueue[func(snapshot pglogrepl.LSN) error]

	genDeleteTombstone    bool
	genReadEvent          bool
	genInsertEvent        bool
	genUpdateEvent        bool
	genDeleteEvent        bool
	genTruncateEvent      bool
	genMessageEvent       bool
	genCompressionEvent   bool
	genDecompressionEvent bool
}

func newLogicalReplicationResolver(config *sysconfig.SystemConfig, dispatcher *eventhandler.Dispatcher,
	systemCatalog *systemcatalog.SystemCatalog) *logicalReplicationResolver {

	return &logicalReplicationResolver{
		dispatcher:    dispatcher,
		systemCatalog: systemCatalog,
		relations:     make(map[uint32]*pglogrepl.RelationMessage, 0),
		eventQueues:   make(map[string]*replicationQueue[func(snapshot pglogrepl.LSN) error], 0),

		genDeleteTombstone:    configuring.GetOrDefault(config.Config, "sink.tombstone", false),
		genReadEvent:          configuring.GetOrDefault(config.Config, "timescaledb.events.read", true),
		genInsertEvent:        configuring.GetOrDefault(config.Config, "timescaledb.events.insert", true),
		genUpdateEvent:        configuring.GetOrDefault(config.Config, "timescaledb.events.update", true),
		genDeleteEvent:        configuring.GetOrDefault(config.Config, "timescaledb.events.delete", true),
		genTruncateEvent:      configuring.GetOrDefault(config.Config, "timescaledb.events.truncate", true),
		genMessageEvent:       configuring.GetOrDefault(config.Config, "timescaledb.events.message", true),
		genCompressionEvent:   configuring.GetOrDefault(config.Config, "timescaledb.events.compression", false),
		genDecompressionEvent: configuring.GetOrDefault(config.Config, "timescaledb.events.decompression", false),
	}
}

func (l *logicalReplicationResolver) OnChunkSnapshotStartedEvent(_ *model.Hypertable, chunk *model.Chunk) error {
	l.eventQueues[chunk.CanonicalName()] = newReplicationQueue[func(snapshot pglogrepl.LSN) error]()
	logger.Printf("Snapshot of %s started", chunk.CanonicalName())
	return nil
}

func (l *logicalReplicationResolver) OnChunkSnapshotFinishedEvent(
	_ *model.Hypertable, chunk *model.Chunk, snapshot pglogrepl.LSN) error {

	queue := l.eventQueues[chunk.CanonicalName()]
	for {
		fn := queue.pop()
		// Initial queue empty, remove it now, to prevent additional messages being enqueued.
		if fn == nil {
			delete(l.eventQueues, chunk.CanonicalName())
			queue.lock()
			break
		}

		if err := fn(snapshot); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// Second round to make sure there wasn't any concurrent writes
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

func (l *logicalReplicationResolver) OnRelationEvent(xld pglogrepl.XLogData, msg *pglogrepl.RelationMessage) error {
	logger.Printf("RELATION MSG: %+v", msg)
	l.relations[msg.RelationID] = msg
	return nil
}

func (l *logicalReplicationResolver) OnBeginEvent(xld pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error {
	logger.Printf("BEGIN MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnCommitEvent(xld pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error {
	logger.Printf("COMMIT MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnInsertEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if model.IsHypertableEvent(rel) {
		return l.onHypertableInsertEvent(msg, newValues)
	}

	if model.IsChunkEvent(rel) {
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

func (l *logicalReplicationResolver) OnUpdateEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage, oldValues, newValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if model.IsHypertableEvent(rel) {
		return l.onHypertableUpdateEvent(msg, oldValues, newValues)
	}

	if model.IsChunkEvent(rel) {
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

func (l *logicalReplicationResolver) OnDeleteEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if model.IsHypertableEvent(rel) {
		return l.onHypertableDeleteEvent(msg, oldValues)
	}

	if model.IsChunkEvent(rel) {
		return l.onChunkDeleteEvent(xld, msg, oldValues)
	}

	if !l.genUpdateEvent {
		return nil
	}

	chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
	if hypertable != nil {
		if err := l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnDeleteEvent(xld, hypertable, chunk, oldValues, false)
					},
				)
			})
		}); err != nil {
			return err
		}

		if l.genDeleteTombstone {
			return l.enqueueOrExecute(chunk, xld, func() error {
				return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
					notificator.NotifyHypertableReplicationEventHandler(
						func(handler eventhandler.HypertableReplicationEventHandler) error {
							return handler.OnDeleteEvent(xld, hypertable, chunk, oldValues, true)
						},
					)
				})
			})
		}
	}

	return nil
}

func (l *logicalReplicationResolver) OnTruncateEvent(xld pglogrepl.XLogData, msg *pglogrepl.TruncateMessage) error {
	if !l.genTruncateEvent {
		return nil
	}

	truncatedHypertables := make([]*model.Hypertable, 0)
	for i := 0; i < int(msg.RelationNum); i++ {
		rel, ok := l.relations[msg.RelationIDs[i]]
		if !ok {
			logger.Fatalf("unknown relation ID %d", msg.RelationIDs[i])
		}

		if model.IsHypertableEvent(rel) || model.IsChunkEvent(rel) {
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

func (l *logicalReplicationResolver) OnMessageEvent(xld pglogrepl.XLogData, msg *decoding.LogicalReplicationMessage) error {
	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyHypertableReplicationEventHandler(
			func(handler eventhandler.HypertableReplicationEventHandler) error {
				return handler.OnMessageEvent(xld, msg)
			},
		)
	})
}

func (l *logicalReplicationResolver) OnTypeEvent(xld pglogrepl.XLogData, msg *pglogrepl.TypeMessage) error {
	logger.Printf("TYPE MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnOriginEvent(xld pglogrepl.XLogData, msg *pglogrepl.OriginMessage) error {
	logger.Printf("ORIGIN MSG: %+v", msg)
	//TODO implement me
	logger.Printf("Origin: %+v", msg)
	return nil
}

func (l *logicalReplicationResolver) onHypertableInsertEvent(
	msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableAddedEvent(msg.RelationID, newValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkInsertEvent(
	msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkAddedEvent(msg.RelationID, newValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableUpdateEvent(msg *pglogrepl.UpdateMessage,
	oldValues map[string]any, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableUpdatedEvent(msg.RelationID, oldValues, newValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkUpdateEvent(msg *pglogrepl.UpdateMessage,
	oldValues map[string]any, newValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkUpdatedEvent(msg.RelationID, oldValues, newValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableDeleteEvent(
	msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableDeletedEvent(msg.RelationID, oldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkDeleteEvent(xld pglogrepl.XLogData,
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

func (l *logicalReplicationResolver) onChunkCompressionEvent(xld pglogrepl.XLogData,
	hypertable *model.Hypertable, rel *pglogrepl.RelationMessage, chunk *model.Chunk) error {

	if !l.genCompressionEvent {
		return nil
	}

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

func (l *logicalReplicationResolver) onChunkDecompressionEvent(xld pglogrepl.XLogData, chunk *model.Chunk) error {
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

func (l *logicalReplicationResolver) enqueueOrExecute(
	chunk *model.Chunk, xld pglogrepl.XLogData, fn func() error) error {

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

func (l *logicalReplicationResolver) isSnapshotting(chunk *model.Chunk) bool {
	_, present := l.eventQueues[chunk.CanonicalName()]
	return present
}

func (l *logicalReplicationResolver) resolveChunkAndHypertable(
	schemaName, tableName string) (*model.Chunk, *model.Hypertable) {

	chunk := l.systemCatalog.FindChunkByName(schemaName, tableName)
	if chunk == nil {
		return nil, nil
	}

	hypertable := l.systemCatalog.FindHypertableById(chunk.HypertableId())
	return chunk, hypertable
}
