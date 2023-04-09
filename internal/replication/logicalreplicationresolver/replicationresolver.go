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
	relations     map[uint32]*decoding.RelationMessage
	eventQueues   map[string]*supporting.Queue[func(snapshot pglogrepl.LSN) error]

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
		relations:     make(map[uint32]*decoding.RelationMessage, 0),
		eventQueues:   make(map[string]*supporting.Queue[func(snapshot pglogrepl.LSN) error], 0),

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
	l.eventQueues[chunk.CanonicalName()] = supporting.NewQueue[func(snapshot pglogrepl.LSN) error]()
	logger.Printf("Snapshot of %s started", chunk.CanonicalName())
	return nil
}

func (l *logicalReplicationResolver) OnChunkSnapshotFinishedEvent(
	_ *model.Hypertable, chunk *model.Chunk, snapshot pglogrepl.LSN) error {

	queue := l.eventQueues[chunk.CanonicalName()]
	for {
		fn := queue.Pop()
		// Initial queue empty, remove it now, to prevent additional messages being enqueued.
		if fn == nil {
			delete(l.eventQueues, chunk.CanonicalName())
			queue.Lock()
			break
		}

		if err := fn(snapshot); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// Second round to make sure there wasn't any concurrent writes
	for {
		fn := queue.Pop()
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

func (l *logicalReplicationResolver) OnRelationEvent(xld pglogrepl.XLogData, msg *decoding.RelationMessage) error {
	logger.Printf("RELATION MSG: %+v", msg)
	l.relations[msg.RelationID] = msg
	return nil
}

func (l *logicalReplicationResolver) OnBeginEvent(xld pglogrepl.XLogData, msg *decoding.BeginMessage) error {
	logger.Printf("BEGIN MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnCommitEvent(xld pglogrepl.XLogData, msg *decoding.CommitMessage) error {
	logger.Printf("COMMIT MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnInsertEvent(xld pglogrepl.XLogData, msg *decoding.InsertMessage) error {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if model.IsHypertableEvent(rel) {
		return l.onHypertableInsertEvent(msg)
	}

	if model.IsChunkEvent(rel) {
		return l.onChunkInsertEvent(msg)
	}

	if !l.genInsertEvent {
		return nil
	}

	if chunk, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnInsertEvent(xld, hypertable, chunk, msg.NewValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *logicalReplicationResolver) OnUpdateEvent(xld pglogrepl.XLogData, msg *decoding.UpdateMessage) error {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if model.IsHypertableEvent(rel) {
		return l.onHypertableUpdateEvent(msg)
	}

	if model.IsChunkEvent(rel) {
		chunkId := msg.NewValues["id"].(int32)
		if chunk, present := l.systemCatalog.FindChunkById(chunkId); present {
			if hypertable, present := l.systemCatalog.FindHypertableById(chunk.HypertableId()); present {
				if chunk.Status() == 0 && (msg.NewValues["status"].(int32)) == 1 {
					if err := l.onChunkCompressionEvent(xld, hypertable, rel, chunk); err != nil {
						return err
					}
				}
			}
		}

		return l.onChunkUpdateEvent(msg)
	}

	if !l.genUpdateEvent {
		return nil
	}

	if chunk, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnUpdateEvent(xld, hypertable, chunk, msg.OldValues, msg.NewValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *logicalReplicationResolver) OnDeleteEvent(xld pglogrepl.XLogData, msg *decoding.DeleteMessage) error {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if model.IsHypertableEvent(rel) {
		return l.onHypertableDeleteEvent(msg)
	}

	if model.IsChunkEvent(rel) {
		return l.onChunkDeleteEvent(xld, msg)
	}

	if !l.genUpdateEvent {
		return nil
	}

	if chunk, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
		if err := l.enqueueOrExecute(chunk, xld, func() error {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnDeleteEvent(xld, hypertable, chunk, msg.OldValues, false)
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
							return handler.OnDeleteEvent(xld, hypertable, chunk, msg.OldValues, true)
						},
					)
				})
			})
		}
	}

	return nil
}

func (l *logicalReplicationResolver) OnTruncateEvent(xld pglogrepl.XLogData, msg *decoding.TruncateMessage) error {
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

		if _, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
			truncatedHypertables = append(truncatedHypertables, hypertable)
		}
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

func (l *logicalReplicationResolver) OnTypeEvent(xld pglogrepl.XLogData, msg *decoding.TypeMessage) error {
	logger.Printf("TYPE MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnOriginEvent(xld pglogrepl.XLogData, msg *decoding.OriginMessage) error {
	logger.Printf("ORIGIN MSG: %+v", msg)
	//TODO implement me
	logger.Printf("Origin: %+v", msg)
	return nil
}

func (l *logicalReplicationResolver) onHypertableInsertEvent(msg *decoding.InsertMessage) error {
	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableAddedEvent(msg.RelationID, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkInsertEvent(msg *decoding.InsertMessage) error {
	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkAddedEvent(msg.RelationID, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableUpdateEvent(msg *decoding.UpdateMessage) error {
	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableUpdatedEvent(msg.RelationID, msg.OldValues, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkUpdateEvent(msg *decoding.UpdateMessage) error {
	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkUpdatedEvent(msg.RelationID, msg.OldValues, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableDeleteEvent(msg *decoding.DeleteMessage) error {
	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableDeletedEvent(msg.RelationID, msg.OldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkDeleteEvent(xld pglogrepl.XLogData, msg *decoding.DeleteMessage) error {
	if id, ok := msg.OldValues["id"].(int32); ok {
		if chunk, present := l.systemCatalog.FindChunkById(id); present {
			if chunk.IsCompressed() {
				if err := l.onChunkDecompressionEvent(xld, chunk); err != nil {
					return err
				}
			}
		}
	}

	return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkDeletedEvent(msg.RelationID, msg.OldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkCompressionEvent(xld pglogrepl.XLogData,
	hypertable *model.Hypertable, rel *decoding.RelationMessage, chunk *model.Chunk) error {

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
	if uncompressedHypertable, _, present := l.systemCatalog.ResolveUncompressedHypertable(hypertableId); present {
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
	}
	return nil
}

func (l *logicalReplicationResolver) enqueueOrExecute(
	chunk *model.Chunk, xld pglogrepl.XLogData, fn func() error) error {

	if l.isSnapshotting(chunk) {
		queue := l.eventQueues[chunk.CanonicalName()]
		if ok := queue.Push(func(snapshot pglogrepl.LSN) error {
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
	schemaName, tableName string) (chunk *model.Chunk, hypertable *model.Hypertable, present bool) {

	if chunk, present := l.systemCatalog.FindChunkByName(schemaName, tableName); present {
		if hypertable, present := l.systemCatalog.FindHypertableById(chunk.HypertableId()); present {
			return chunk, hypertable, true
		}
	}
	return nil, nil, false
}
