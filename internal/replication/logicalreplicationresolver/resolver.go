package logicalreplicationresolver

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
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

func NewLogicalReplicationResolver(config *configuration.Config, dispatcher *eventhandler.Dispatcher,
	systemCatalog *systemcatalog.SystemCatalog) *LogicalReplicationResolver {

	return &LogicalReplicationResolver{
		dispatcher:    dispatcher,
		systemCatalog: systemCatalog,
		relations:     make(map[uint32]*pglogrepl.RelationMessage, 0),
		eventQueues:   make(map[string]*replicationQueue, 0),

		genReadEvent:          configuration.GetOrDefault(config, "timescaledb.events.read", true),
		genInsertEvent:        configuration.GetOrDefault(config, "timescaledb.events.insert", true),
		genUpdateEvent:        configuration.GetOrDefault(config, "timescaledb.events.update", true),
		genDeleteEvent:        configuration.GetOrDefault(config, "timescaledb.events.delete", true),
		genTruncateEvent:      configuration.GetOrDefault(config, "timescaledb.events.truncate", true),
		genCompressionEvent:   configuration.GetOrDefault(config, "timescaledb.events.compression", true),
		genDecompressionEvent: configuration.GetOrDefault(config, "timescaledb.events.decompression", true),
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
	l.relations[msg.RelationID] = msg
	return nil
}

func (l *LogicalReplicationResolver) OnBeginEvent(xld pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error {
	//TODO implement me
	return nil
}

func (l *LogicalReplicationResolver) OnCommitEvent(xld pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error {
	//TODO implement me
	return nil
}

func (l *LogicalReplicationResolver) OnInsertEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage, newValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if rel.Namespace == "_timescaledb_catalog" {
		if rel.RelationName == "hypertable" {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifySystemCatalogReplicationEventHandler(
					func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
						return handler.OnHypertableAddedEvent(msg.RelationID, newValues)
					},
				)
			})
		} else if rel.RelationName == "chunk" {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifySystemCatalogReplicationEventHandler(
					func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
						return handler.OnChunkAddedEvent(msg.RelationID, newValues)
					},
				)
			})
		}
	} else {
		chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
		if hypertable != nil {
			if hypertable.IsCompressedTable() {
				uncompressedHypertable := l.systemCatalog.FindHypertableByCompressedHypertableId(hypertable.Id())
				logger.Printf(
					"COMPRESSION EVENT %s.%s FOR CHUNK %s.%s", uncompressedHypertable.SchemaName(),
					uncompressedHypertable.HypertableName(), rel.Namespace, rel.RelationName,
				)

				if !l.genCompressionEvent {
					return nil
				}

				return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
					notificator.NotifyCompressionReplicationEventHandler(
						func(handler eventhandler.CompressionReplicationEventHandler) error {
							return handler.OnChunkCompressedEvent(xld, hypertable, chunk)
						},
					)
				})
			} else {
				if !l.genInsertEvent {
					return nil
				}

				enqueue := func() error {
					return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
						notificator.NotifyHypertableReplicationEventHandler(
							func(handler eventhandler.HypertableReplicationEventHandler) error {
								return handler.OnInsertEvent(xld, hypertable, chunk, newValues)
							},
						)
					})
				}

				if l.isSnapshotting(chunk) {
					queue := l.eventQueues[chunk.CanonicalName()]
					if ok := queue.push(func(snapshot pglogrepl.LSN) error {
						if xld.ServerWALEnd < snapshot {
							return nil
						}
						return enqueue()
					}); ok {
						return nil
					}
				}

				if chunk.CompressedChunkId() != nil && !chunk.IsPartiallyCompressed() {
					return nil
				}
				return enqueue()
			}
		}
	}
	return nil
}

func (l *LogicalReplicationResolver) OnUpdateEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage, oldValues, newValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if rel.Namespace == "_timescaledb_catalog" {
		if rel.RelationName == "hypertable" {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifySystemCatalogReplicationEventHandler(
					func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
						return handler.OnHypertableUpdatedEvent(msg.RelationID, oldValues, newValues)
					},
				)
			})
		} else if rel.RelationName == "chunk" {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifySystemCatalogReplicationEventHandler(
					func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
						return handler.OnChunkUpdatedEvent(msg.RelationID, oldValues, newValues)
					},
				)
			})
		}
	} else {
		if !l.genUpdateEvent {
			return nil
		}

		chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
		if hypertable != nil {
			enqueue := func() error {
				return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
					notificator.NotifyHypertableReplicationEventHandler(
						func(handler eventhandler.HypertableReplicationEventHandler) error {
							return handler.OnUpdateEvent(xld, hypertable, chunk, oldValues, newValues)
						},
					)
				})
			}

			if l.isSnapshotting(chunk) {
				queue := l.eventQueues[chunk.CanonicalName()]
				if ok := queue.push(func(snapshot pglogrepl.LSN) error {
					if xld.ServerWALEnd < snapshot {
						return nil
					}
					return enqueue()
				}); ok {
					return nil
				}
			}

			return enqueue()
		}
	}
	return nil
}

func (l *LogicalReplicationResolver) OnDeleteEvent(
	xld pglogrepl.XLogData, msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	rel, ok := l.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if rel.Namespace == "_timescaledb_catalog" {
		if rel.RelationName == "hypertable" {
			return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifySystemCatalogReplicationEventHandler(
					func(handler eventhandler.SystemCatalogReplicationEventHandler) error {
						return handler.OnHypertableDeletedEvent(msg.RelationID, oldValues)
					},
				)
			})
		} else if rel.RelationName == "chunk" {
			if id, ok := oldValues["id"].(int32); ok {
				chunk := l.systemCatalog.FindChunkById(id)
				if chunk.IsCompressed() {
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
	} else {
		if !l.genUpdateEvent {
			return nil
		}

		chunk, hypertable := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName)
		if hypertable != nil {
			enqueue := func() error {
				return l.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
					notificator.NotifyHypertableReplicationEventHandler(
						func(handler eventhandler.HypertableReplicationEventHandler) error {
							return handler.OnDeleteEvent(xld, hypertable, chunk, oldValues)
						},
					)
				})
			}

			if l.isSnapshotting(chunk) {
				queue := l.eventQueues[chunk.CanonicalName()]
				if ok := queue.push(func(snapshot pglogrepl.LSN) error {
					if xld.ServerWALEnd < snapshot {
						return nil
					}
					return enqueue()
				}); ok {
					return nil
				}
			}

			return enqueue()
		}
	}
	return nil
}

func (l *LogicalReplicationResolver) OnTruncateEvent(xld pglogrepl.XLogData, msg *pglogrepl.TruncateMessage) error {
	if !l.genTruncateEvent {
		return nil
	}

	//TODO implement me
	logger.Printf("Truncate: %+v", msg)
	return nil
}

func (l *LogicalReplicationResolver) OnTypeEvent(xld pglogrepl.XLogData, msg *pglogrepl.TypeMessage) error {
	//TODO implement me
	return nil
}

func (l *LogicalReplicationResolver) OnOriginEvent(xld pglogrepl.XLogData, msg *pglogrepl.OriginMessage) error {
	//TODO implement me
	logger.Printf("Origin: %+v", msg)
	return nil
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
