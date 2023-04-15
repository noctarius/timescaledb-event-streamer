package logicalreplicationresolver

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	spicatalog "github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type logicalReplicationResolver struct {
	replicationContext *context.ReplicationContext
	systemCatalog      *systemcatalog.SystemCatalog
	relations          map[uint32]*pgtypes.RelationMessage
	eventQueues        map[string]*supporting.Queue[func(snapshot pglogrepl.LSN) error]
	logger             *logging.Logger

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

func newLogicalReplicationResolver(config *sysconfig.SystemConfig, replicationContext *context.ReplicationContext,
	systemCatalog *systemcatalog.SystemCatalog) *logicalReplicationResolver {

	return &logicalReplicationResolver{
		replicationContext: replicationContext,
		systemCatalog:      systemCatalog,
		relations:          make(map[uint32]*pgtypes.RelationMessage, 0),
		eventQueues:        make(map[string]*supporting.Queue[func(snapshot pglogrepl.LSN) error], 0),
		logger:             logging.NewLogger("LogicalReplicationResolver"),

		genDeleteTombstone:    spiconfig.GetOrDefault(config.Config, spiconfig.PropertySinkTombstone, false),
		genReadEvent:          spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsRead, true),
		genInsertEvent:        spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsInsert, true),
		genUpdateEvent:        spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsUpdate, true),
		genDeleteEvent:        spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsDelete, true),
		genTruncateEvent:      spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsTruncate, true),
		genMessageEvent:       spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsMessage, true),
		genCompressionEvent:   spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsCompression, false),
		genDecompressionEvent: spiconfig.GetOrDefault(config.Config, spiconfig.PropertyEventsDecompression, false),
	}
}

func (l *logicalReplicationResolver) OnChunkSnapshotStartedEvent(
	_ *spicatalog.Hypertable, chunk *spicatalog.Chunk) error {

	l.eventQueues[chunk.CanonicalName()] = supporting.NewQueue[func(snapshot pglogrepl.LSN) error]()
	l.logger.Infof("Snapshot of %s started", chunk.CanonicalName())
	return nil
}

func (l *logicalReplicationResolver) OnChunkSnapshotFinishedEvent(
	_ *spicatalog.Hypertable, chunk *spicatalog.Chunk, snapshot pglogrepl.LSN) error {

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
	l.logger.Infof("Snapshot of %s finished", chunk.CanonicalName())

	return nil
}

func (l *logicalReplicationResolver) OnRelationEvent(xld pglogrepl.XLogData, msg *pgtypes.RelationMessage) error {
	l.logger.Debugf("RELATION MSG: %+v", msg)
	l.relations[msg.RelationID] = msg
	return nil
}

func (l *logicalReplicationResolver) OnBeginEvent(xld pglogrepl.XLogData, msg *pgtypes.BeginMessage) error {
	l.logger.Debugf("BEGIN MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnCommitEvent(xld pglogrepl.XLogData, msg *pgtypes.CommitMessage) error {
	l.logger.Debugf("COMMIT MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnInsertEvent(xld pglogrepl.XLogData, msg *pgtypes.InsertMessage) error {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		l.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if spicatalog.IsHypertableEvent(rel) {
		return l.onHypertableInsertEvent(msg)
	}

	if spicatalog.IsChunkEvent(rel) {
		return l.onChunkInsertEvent(msg)
	}

	if !l.genInsertEvent {
		return nil
	}

	if chunk, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnInsertEvent(xld, hypertable, chunk, msg.NewValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *logicalReplicationResolver) OnUpdateEvent(xld pglogrepl.XLogData, msg *pgtypes.UpdateMessage) error {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		l.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if spicatalog.IsHypertableEvent(rel) {
		return l.onHypertableUpdateEvent(msg)
	}

	if spicatalog.IsChunkEvent(rel) {
		chunkId := msg.NewValues["id"].(int32)
		if chunk, present := l.systemCatalog.FindChunkById(chunkId); present {
			if chunk.Status() == 0 && (msg.NewValues["status"].(int32)) == 1 {
				if err := l.onChunkCompressionEvent(xld, chunk); err != nil {
					return err
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
			return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnUpdateEvent(xld, hypertable, chunk, msg.OldValues, msg.NewValues)
					},
				)
			})
		})
	}

	return nil
}

func (l *logicalReplicationResolver) OnDeleteEvent(xld pglogrepl.XLogData, msg *pgtypes.DeleteMessage) error {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		l.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if spicatalog.IsHypertableEvent(rel) {
		return l.onHypertableDeleteEvent(msg)
	}

	if spicatalog.IsChunkEvent(rel) {
		return l.onChunkDeleteEvent(xld, msg)
	}

	if !l.genUpdateEvent {
		return nil
	}

	if chunk, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
		if err := l.enqueueOrExecute(chunk, xld, func() error {
			return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnDeleteEvent(xld, hypertable, chunk, msg.OldValues, false)
					},
				)
			})
		}); err != nil {
			return err
		}

		if l.genDeleteTombstone {
			return l.enqueueOrExecute(chunk, xld, func() error {
				return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
					notificator.NotifyHypertableReplicationEventHandler(
						func(handler eventhandlers.HypertableReplicationEventHandler) error {
							return handler.OnDeleteEvent(xld, hypertable, chunk, msg.OldValues, true)
						},
					)
				})
			})
		}
	}

	return nil
}

func (l *logicalReplicationResolver) OnTruncateEvent(xld pglogrepl.XLogData, msg *pgtypes.TruncateMessage) error {
	if !l.genTruncateEvent {
		return nil
	}

	truncatedHypertables := make([]*spicatalog.Hypertable, 0)
	for i := 0; i < int(msg.RelationNum); i++ {
		rel, ok := l.relations[msg.RelationIDs[i]]
		if !ok {
			l.logger.Fatalf("unknown relation ID %d", msg.RelationIDs[i])
		}

		if spicatalog.IsHypertableEvent(rel) || spicatalog.IsChunkEvent(rel) {
			// Catalog tables shouldn't be truncated; EVER!
			continue
		}

		if _, hypertable, present := l.resolveChunkAndHypertable(rel.Namespace, rel.RelationName); present {
			truncatedHypertables = append(truncatedHypertables, hypertable)
		}
	}

	truncatedHypertables = supporting.DistinctItems(truncatedHypertables, func(item *spicatalog.Hypertable) string {
		return item.CanonicalName()
	})

	for _, hypertable := range truncatedHypertables {
		if err := l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyHypertableReplicationEventHandler(
				func(handler eventhandlers.HypertableReplicationEventHandler) error {
					return handler.OnTruncateEvent(xld, hypertable)
				},
			)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logicalReplicationResolver) OnMessageEvent(
	xld pglogrepl.XLogData, msg *pgtypes.LogicalReplicationMessage) error {

	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifyHypertableReplicationEventHandler(
			func(handler eventhandlers.HypertableReplicationEventHandler) error {
				return handler.OnMessageEvent(xld, msg)
			},
		)
	})
}

func (l *logicalReplicationResolver) OnTypeEvent(xld pglogrepl.XLogData, msg *pgtypes.TypeMessage) error {
	l.logger.Debugf("TYPE MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnOriginEvent(xld pglogrepl.XLogData, msg *pgtypes.OriginMessage) error {
	l.logger.Debugf("ORIGIN MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) onHypertableInsertEvent(msg *pgtypes.InsertMessage) error {
	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableAddedEvent(msg.RelationID, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkInsertEvent(msg *pgtypes.InsertMessage) error {
	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkAddedEvent(msg.RelationID, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableUpdateEvent(msg *pgtypes.UpdateMessage) error {
	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableUpdatedEvent(msg.RelationID, msg.OldValues, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkUpdateEvent(msg *pgtypes.UpdateMessage) error {
	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkUpdatedEvent(msg.RelationID, msg.OldValues, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableDeleteEvent(msg *pgtypes.DeleteMessage) error {
	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableDeletedEvent(msg.RelationID, msg.OldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkDeleteEvent(xld pglogrepl.XLogData, msg *pgtypes.DeleteMessage) error {
	if id, ok := msg.OldValues["id"].(int32); ok {
		if chunk, present := l.systemCatalog.FindChunkById(id); present {
			if chunk.IsCompressed() {
				if err := l.onChunkDecompressionEvent(xld, chunk); err != nil {
					return err
				}
			}
		}
	}

	return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkDeletedEvent(msg.RelationID, msg.OldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkCompressionEvent(xld pglogrepl.XLogData, chunk *spicatalog.Chunk) error {
	hypertableId := chunk.HypertableId()
	if uncompressedHypertable, _, present := l.systemCatalog.ResolveUncompressedHypertable(hypertableId); present {
		l.logger.Infof(
			"COMPRESSION EVENT %s.%s FOR CHUNK %s.%s", uncompressedHypertable.SchemaName(),
			uncompressedHypertable.TableName(), chunk.SchemaName(), chunk.TableName(),
		)

		if !l.genCompressionEvent {
			return nil
		}

		return l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyCompressionReplicationEventHandler(
				func(handler eventhandlers.CompressionReplicationEventHandler) error {
					return handler.OnChunkCompressedEvent(xld, uncompressedHypertable, chunk)
				},
			)
		})
	}
	return nil
}

func (l *logicalReplicationResolver) onChunkDecompressionEvent(xld pglogrepl.XLogData, chunk *spicatalog.Chunk) error {
	hypertableId := chunk.HypertableId()
	if uncompressedHypertable, _, present := l.systemCatalog.ResolveUncompressedHypertable(hypertableId); present {
		l.logger.Infof(
			"DECOMPRESSION EVENT %s.%s FOR CHUNK %s.%s", uncompressedHypertable.SchemaName(),
			uncompressedHypertable.TableName(), chunk.SchemaName(), chunk.TableName(),
		)

		if !l.genDecompressionEvent {
			return nil
		}

		if err := l.replicationContext.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyCompressionReplicationEventHandler(
				func(handler eventhandlers.CompressionReplicationEventHandler) error {
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
	chunk *spicatalog.Chunk, xld pglogrepl.XLogData, fn func() error) error {

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

func (l *logicalReplicationResolver) isSnapshotting(chunk *spicatalog.Chunk) bool {
	_, present := l.eventQueues[chunk.CanonicalName()]
	return present
}

func (l *logicalReplicationResolver) resolveChunkAndHypertable(
	schemaName, tableName string) (*spicatalog.Chunk, *spicatalog.Hypertable, bool) {

	if chunk, present := l.systemCatalog.FindChunkByName(schemaName, tableName); present {
		if hypertable, present := l.systemCatalog.FindHypertableById(chunk.HypertableId()); present {
			return chunk, hypertable, true
		}
	}
	return nil, nil, false
}
