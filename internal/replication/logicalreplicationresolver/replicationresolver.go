/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package logicalreplicationresolver

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/containers"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	spicatalog "github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"github.com/samber/lo"
)

type snapshotCallback func(snapshot pgtypes.LSN) error

type logicalReplicationResolver struct {
	replicationContext replicationcontext.ReplicationContext
	systemCatalog      systemcatalog.SystemCatalog
	taskManager        task.TaskManager
	typeManager        pgtypes.TypeManager
	logger             *logging.Logger

	decompressionEventTaskOperation func(task.Task) error

	relations     *containers.RelationCache[*pgtypes.RelationMessage]
	chunkIdLookup *containers.RelationCache[int32]
	eventQueues   map[string]*containers.Queue[snapshotCallback]

	genDeleteTombstone              bool
	genHypertableReadEvent          bool
	genHypertableInsertEvent        bool
	genHypertableUpdateEvent        bool
	genHypertableDeleteEvent        bool
	genHypertableTruncateEvent      bool
	genHypertableCompressionEvent   bool
	genHypertableDecompressionEvent bool

	genPostgresqlReadEvent     bool
	genPostgresqlInsertEvent   bool
	genPostgresqlUpdateEvent   bool
	genPostgresqlDeleteEvent   bool
	genPostgresqlTruncateEvent bool

	genMessageEvent bool
}

func newLogicalReplicationResolver(
	config *spiconfig.Config, replicationContext replicationcontext.ReplicationContext,
	systemCatalog systemcatalog.SystemCatalog, typeManager pgtypes.TypeManager,
	taskManager task.TaskManager,
) (*logicalReplicationResolver, error) {

	logger, err := logging.NewLogger("LogicalReplicationResolver")
	if err != nil {
		return nil, err
	}

	genHypertableMessageEvent := spiconfig.GetOrDefault(config, spiconfig.PropertyHypertableEventsMessage, true)
	genPostgresqlMessageEvent := spiconfig.GetOrDefault(config, spiconfig.PropertyPostgresqlEventsMessage, true)

	// There's a difference in the order of event enqueuing pre-, and post TimescaleDB 2.12
	// due to the introduction of decompression markers. Therefore, pre 2.12 we need to
	// schedule the decompression event to the end of the operation queue, while with 2.12+
	// we need to execute it immediately (if decompression markers are enabled that is).
	decompressionEventTaskOperation := taskManager.EnqueueTask
	if replicationContext.IsDecompressionMarkingEnabled() {
		decompressionEventTaskOperation = taskManager.RunTask
	}

	return &logicalReplicationResolver{
		replicationContext: replicationContext,
		systemCatalog:      systemCatalog,
		taskManager:        taskManager,
		typeManager:        typeManager,
		logger:             logger,

		decompressionEventTaskOperation: decompressionEventTaskOperation,

		relations:     containers.NewRelationCache[*pgtypes.RelationMessage](),
		chunkIdLookup: containers.NewRelationCache[int32](),
		eventQueues:   make(map[string]*containers.Queue[snapshotCallback]),

		genDeleteTombstone: spiconfig.GetOrDefault(config, spiconfig.PropertySinkTombstone, false),

		genMessageEvent: genHypertableMessageEvent || genPostgresqlMessageEvent,

		genHypertableReadEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsRead, true,
		),
		genHypertableInsertEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsInsert, true,
		),
		genHypertableUpdateEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsUpdate, true,
		),
		genHypertableDeleteEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsDelete, true,
		),
		genHypertableTruncateEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsTruncate, true,
		),
		genHypertableCompressionEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsCompression, false,
		),
		genHypertableDecompressionEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyHypertableEventsDecompression, false,
		),

		genPostgresqlReadEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyPostgresqlEventsRead, true,
		),
		genPostgresqlInsertEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyPostgresqlEventsInsert, true,
		),
		genPostgresqlUpdateEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyPostgresqlEventsUpdate, true,
		),
		genPostgresqlDeleteEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyPostgresqlEventsDelete, true,
		),
		genPostgresqlTruncateEvent: spiconfig.GetOrDefault(
			config, spiconfig.PropertyPostgresqlEventsTruncate, true,
		),
	}, nil
}

func (l *logicalReplicationResolver) PostConstruct() error {
	l.taskManager.RegisterReplicationEventHandler(l)
	return nil
}

func (l *logicalReplicationResolver) OnTableSnapshotStartedEvent(
	_ string, _ spicatalog.BaseTable,
) error {

	return nil
}

func (l *logicalReplicationResolver) OnTableSnapshotFinishedEvent(
	_ string, _ spicatalog.BaseTable, _ pgtypes.LSN,
) error {

	return nil
}

func (l *logicalReplicationResolver) OnSnapshottingStartedEvent(
	_ string,
) error {

	return nil
}

func (l *logicalReplicationResolver) OnSnapshottingFinishedEvent() error {
	// FIXME: Kick off the actual replication loop
	return nil
}

func (l *logicalReplicationResolver) OnChunkSnapshotStartedEvent(
	_ *spicatalog.Hypertable, chunk *spicatalog.Chunk,
) error {

	l.eventQueues[chunk.CanonicalName()] = containers.NewQueue[snapshotCallback](1_000_000)
	l.logger.Infof("Snapshot of %s started", chunk.CanonicalName())
	return nil
}

func (l *logicalReplicationResolver) OnChunkSnapshotFinishedEvent(
	_ *spicatalog.Hypertable, chunk *spicatalog.Chunk, snapshot pgtypes.LSN,
) error {

	queue := l.eventQueues[chunk.CanonicalName()]
	for {
		fn := queue.Pop()
		// If queue empty, remove it now, to prevent additional messages being enqueued.
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
			queue.Close()
			break
		}

		if err := fn(snapshot); err != nil {
			return errors.Wrap(err, 0)
		}
	}
	l.logger.Infof("Snapshot of %s finished", chunk.CanonicalName())

	return nil
}

func (l *logicalReplicationResolver) OnRelationEvent(
	_ pgtypes.XLogData, msg *pgtypes.RelationMessage,
) error {

	l.relations.Set(msg.RelationID, msg)
	if _, err := l.typeManager.GetOrPlanTupleDecoder(msg); err != nil {
		return err
	}
	return nil
}

func (l *logicalReplicationResolver) OnBeginEvent(
	xld pgtypes.XLogData, msg *pgtypes.BeginMessage,
) error {

	l.replicationContext.SetLastBeginLSN(pgtypes.LSN(xld.WALStart))
	l.replicationContext.SetLastTransactionId(msg.Xid)
	return nil
}

func (l *logicalReplicationResolver) OnCommitEvent(
	xld pgtypes.XLogData, msg *pgtypes.CommitMessage,
) error {

	l.replicationContext.SetLastCommitLSN(pgtypes.LSN(msg.TransactionEndLSN))
	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifyRecordReplicationEventHandler(
			func(handler eventhandlers.RecordReplicationEventHandler) error {
				return handler.OnTransactionFinishedEvent(xld, msg)
			},
		)
	})
}

func (l *logicalReplicationResolver) OnInsertEvent(
	xld pgtypes.XLogData, msg *pgtypes.InsertMessage,
) error {

	rel, present := l.relations.Get(msg.RelationID)
	if !present {
		l.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if spicatalog.IsHypertableEvent(rel) {
		return l.onHypertableInsertEvent(xld, msg)
	}

	if spicatalog.IsChunkEvent(rel) {
		return l.onChunkInsertEvent(xld, msg)
	}

	var table schema.TableAlike
	var chunk *systemcatalog.Chunk

	if spicatalog.IsVanillaTable(rel) {
		if !l.genPostgresqlInsertEvent {
			return nil
		}
		t, present := l.systemCatalog.FindVanillaTableById(rel.RelationID)
		if !present {
			return nil
		}
		table = t
	} else {
		if !l.genHypertableInsertEvent {
			return nil
		}

		c, h, present := l.resolveChunkAndHypertable(
			rel.RelationID, rel.Namespace, rel.RelationName,
		)
		if !present {
			return nil
		}

		table = h
		chunk = c
	}

	return l.enqueueOrExecute(chunk, xld, func() error {
		return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifyRecordReplicationEventHandler(
				func(handler eventhandlers.RecordReplicationEventHandler) error {
					return handler.OnInsertEvent(xld, table, chunk, msg.NewValues)
				},
			)
		})
	})
}

func (l *logicalReplicationResolver) OnUpdateEvent(
	xld pgtypes.XLogData, msg *pgtypes.UpdateMessage,
) error {

	rel, present := l.relations.Get(msg.RelationID)
	if !present {
		l.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	if spicatalog.IsHypertableEvent(rel) {
		return l.onHypertableUpdateEvent(xld, msg)
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

		return l.onChunkUpdateEvent(xld, msg)
	}

	var table schema.TableAlike
	var chunk *systemcatalog.Chunk

	if spicatalog.IsVanillaTable(rel) {
		if !l.genPostgresqlUpdateEvent {
			return nil
		}

		t, present := l.systemCatalog.FindVanillaTableById(rel.RelationID)
		if !present {
			return nil
		}
		table = t
	} else {
		if !l.genHypertableUpdateEvent {
			return nil
		}

		c, h, present := l.resolveChunkAndHypertable(
			rel.RelationID, rel.Namespace, rel.RelationName,
		)
		if !present {
			return nil
		}

		table = h
		chunk = c
	}

	return l.enqueueOrExecute(chunk, xld, func() error {
		return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifyRecordReplicationEventHandler(
				func(handler eventhandlers.RecordReplicationEventHandler) error {
					return handler.OnUpdateEvent(xld, table, chunk, msg.OldValues, msg.NewValues)
				},
			)
		})
	})
}

func (l *logicalReplicationResolver) OnDeleteEvent(
	xld pgtypes.XLogData, msg *pgtypes.DeleteMessage,
) error {

	rel, present := l.relations.Get(msg.RelationID)
	if !present {
		l.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	if spicatalog.IsHypertableEvent(rel) {
		return l.onHypertableDeleteEvent(xld, msg)
	}

	if spicatalog.IsChunkEvent(rel) {
		return l.onChunkDeleteEvent(xld, msg)
	}

	var table schema.TableAlike
	var chunk *systemcatalog.Chunk

	if spicatalog.IsVanillaTable(rel) {
		if !l.genPostgresqlDeleteEvent {
			return nil
		}

		t, present := l.systemCatalog.FindVanillaTableById(rel.RelationID)
		if !present {
			return nil
		}
		table = t
	} else {
		if !l.genHypertableDeleteEvent {
			return nil
		}

		c, h, present := l.resolveChunkAndHypertable(
			rel.RelationID, rel.Namespace, rel.RelationName,
		)
		if !present {
			return nil
		}

		table = h
		chunk = c
	}

	if err := l.enqueueOrExecute(chunk, xld, func() error {
		return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifyRecordReplicationEventHandler(
				func(handler eventhandlers.RecordReplicationEventHandler) error {
					return handler.OnDeleteEvent(xld, table, chunk, msg.OldValues, false)
				},
			)
		})
	}); err != nil {
		return err
	}

	if l.genDeleteTombstone {
		return l.enqueueOrExecute(chunk, xld, func() error {
			return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
				notificator.NotifyRecordReplicationEventHandler(
					func(handler eventhandlers.RecordReplicationEventHandler) error {
						return handler.OnDeleteEvent(xld, table, chunk, msg.OldValues, true)
					},
				)
			})
		})
	}

	return nil
}

func (l *logicalReplicationResolver) OnTruncateEvent(
	xld pgtypes.XLogData, msg *pgtypes.TruncateMessage,
) error {

	unknownRelations := lo.Filter(msg.RelationIDs, func(relId uint32, _ int) bool {
		_, present := l.relations.Get(relId)
		if !present {
			l.logger.Fatalf("unknown relation ID %d", relId)
			return true
		}
		return false
	})
	affectedTablesVanilla := lo.Filter(msg.RelationIDs, func(relId uint32, _ int) bool {
		if relation, present := l.relations.Get(relId); present {
			return spicatalog.IsVanillaTable(relation)
		}
		return false
	})
	affectedTablesHypertables := lo.Filter(msg.RelationIDs, func(relId uint32, _ int) bool {
		return !lo.Contains(affectedTablesVanilla, relId) && !lo.Contains(unknownRelations, relId)
	})

	// FIXME: Truncate support for vanilla tables missing!

	truncatedTables := make([]schema.TableAlike, 0)
	for i := 0; i < int(msg.RelationNum); i++ {
		relId := msg.RelationIDs[i]
		if lo.Contains(affectedTablesHypertables, relId) {
			if !l.genHypertableTruncateEvent {
				continue
			}

			rel, present := l.relations.Get(relId)
			if !present {
				l.logger.Fatalf("unknown relation ID %d", msg.RelationIDs[i])
			}

			if spicatalog.IsHypertableEvent(rel) || spicatalog.IsChunkEvent(rel) {
				// Catalog tables shouldn't be truncated; EVER!
				continue
			}

			if _, hypertable, present := l.resolveChunkAndHypertable(
				rel.RelationID, rel.Namespace, rel.RelationName,
			); present {

				truncatedTables = append(truncatedTables, hypertable)
			}
		}

		if lo.Contains(affectedTablesVanilla, relId) {
			if !l.genPostgresqlTruncateEvent {
				continue
			}

			if table, present := l.systemCatalog.FindVanillaTableById(relId); present {
				truncatedTables = append(truncatedTables, table)
			}
		}
	}

	truncatedTables = lo.UniqBy(truncatedTables, schema.TableAlike.CanonicalName)
	for _, hypertable := range truncatedTables {
		if err := l.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifyRecordReplicationEventHandler(
				func(handler eventhandlers.RecordReplicationEventHandler) error {
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
	xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage,
) error {

	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifyRecordReplicationEventHandler(
			func(handler eventhandlers.RecordReplicationEventHandler) error {
				return handler.OnMessageEvent(xld, msg)
			},
		)
	})
}

func (l *logicalReplicationResolver) OnTypeEvent(
	xld pgtypes.XLogData, msg *pgtypes.TypeMessage,
) error {

	l.logger.Debugf("TYPE MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) OnOriginEvent(
	xld pgtypes.XLogData, msg *pgtypes.OriginMessage,
) error {

	l.logger.Debugf("ORIGIN MSG: %+v", msg)
	//TODO implement me
	return nil
}

func (l *logicalReplicationResolver) onHypertableInsertEvent(
	xld pgtypes.XLogData, msg *pgtypes.InsertMessage,
) error {

	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableAddedEvent(xld, msg.RelationID, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkInsertEvent(
	xld pgtypes.XLogData, msg *pgtypes.InsertMessage,
) error {

	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkAddedEvent(xld, msg.RelationID, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableUpdateEvent(
	xld pgtypes.XLogData, msg *pgtypes.UpdateMessage,
) error {

	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableUpdatedEvent(xld, msg.RelationID, msg.OldValues, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkUpdateEvent(
	xld pgtypes.XLogData, msg *pgtypes.UpdateMessage,
) error {

	return l.taskManager.RunTask(func(notificator task.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkUpdatedEvent(xld, msg.RelationID, msg.OldValues, msg.NewValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onHypertableDeleteEvent(
	xld pgtypes.XLogData, msg *pgtypes.DeleteMessage,
) error {

	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnHypertableDeletedEvent(xld, msg.RelationID, msg.OldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkDeleteEvent(
	xld pgtypes.XLogData, msg *pgtypes.DeleteMessage,
) error {

	if id, ok := msg.OldValues["id"].(int32); ok {
		if chunk, present := l.systemCatalog.FindChunkById(id); present {
			if chunk.IsCompressed() {
				if err := l.onChunkDecompressionEvent(xld, chunk); err != nil {
					return err
				}
			}
		}
	}

	return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySystemCatalogReplicationEventHandler(
			func(handler eventhandlers.SystemCatalogReplicationEventHandler) error {
				return handler.OnChunkDeletedEvent(xld, msg.RelationID, msg.OldValues)
			},
		)
	})
}

func (l *logicalReplicationResolver) onChunkCompressionEvent(
	xld pgtypes.XLogData, chunk *spicatalog.Chunk,
) error {

	hypertableId := chunk.HypertableId()
	if uncompressedHypertable, _, present := l.systemCatalog.ResolveUncompressedHypertable(hypertableId); present {
		l.logger.Infof(
			"COMPRESSION EVENT %s.%s FOR CHUNK %s.%s", uncompressedHypertable.SchemaName(),
			uncompressedHypertable.TableName(), chunk.SchemaName(), chunk.TableName(),
		)

		if !l.genHypertableCompressionEvent {
			return nil
		}

		return l.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifyCompressionReplicationEventHandler(
				func(handler eventhandlers.CompressionReplicationEventHandler) error {
					return handler.OnChunkCompressedEvent(xld, uncompressedHypertable, chunk)
				},
			)
		})
	}
	return nil
}

func (l *logicalReplicationResolver) onChunkDecompressionEvent(
	xld pgtypes.XLogData, chunk *spicatalog.Chunk,
) error {

	hypertableId := chunk.HypertableId()
	if uncompressedHypertable, _, present := l.systemCatalog.ResolveUncompressedHypertable(hypertableId); present {
		l.logger.Infof(
			"DECOMPRESSION EVENT %s.%s FOR CHUNK %s.%s", uncompressedHypertable.SchemaName(),
			uncompressedHypertable.TableName(), chunk.SchemaName(), chunk.TableName(),
		)

		if !l.genHypertableDecompressionEvent {
			return nil
		}

		if err := l.decompressionEventTaskOperation(func(notificator task.Notificator) {
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
	chunk *spicatalog.Chunk, xld pgtypes.XLogData, fn func() error,
) error {

	if l.isSnapshotting(chunk) {
		queue := l.eventQueues[chunk.CanonicalName()]
		if ok := queue.Push(func(snapshot pgtypes.LSN) error {
			if xld.ServerWALEnd < pglogrepl.LSN(snapshot) {
				return nil
			}
			return fn()
		}); ok {
			return nil
		}
	}

	return fn()
}

func (l *logicalReplicationResolver) isSnapshotting(
	chunk *spicatalog.Chunk,
) bool {

	if chunk == nil {
		return false
	}

	_, present := l.eventQueues[chunk.CanonicalName()]
	return present
}

func (l *logicalReplicationResolver) resolveChunkAndHypertable(
	chunkOid uint32, schemaName, tableName string,
) (*spicatalog.Chunk, *spicatalog.Hypertable, bool) {

	var chunk *systemcatalog.Chunk

	chunkId, present := l.chunkIdLookup.Get(chunkOid)
	if !present {
		chunk, present = l.systemCatalog.FindChunkByName(schemaName, tableName)
		if !present {
			return nil, nil, false
		}
		l.chunkIdLookup.Set(chunkOid, chunk.Id())
	}

	if chunk == nil {
		chunk, present = l.systemCatalog.FindChunkById(chunkId)
		if !present {
			return nil, nil, false
		}
	}

	if hypertable, present := l.systemCatalog.FindHypertableById(chunk.HypertableId()); present {
		return chunk, hypertable, true
	}
	return nil, nil, false
}
