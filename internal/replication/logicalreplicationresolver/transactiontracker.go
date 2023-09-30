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
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/containers"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	spicatalog "github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"time"
)

const (
	decompressionMarkerStartId = "::timescaledb-decompression-start"
	decompressionMarkerEndId   = "::timescaledb-decompression-end"
)

type transactionTracker struct {
	timeout                      time.Duration
	relations                    *containers.RelationCache[*pgtypes.RelationMessage]
	resolver                     *logicalReplicationResolver
	taskManager                  task.TaskManager
	systemCatalog                systemcatalog.SystemCatalog
	activeTransaction            *transaction
	logger                       *logging.Logger
	supportsDecompressionMarkers bool
}

func newTransactionTracker(
	timeout time.Duration, maxSize uint, replicationContext replicationcontext.ReplicationContext,
	systemCatalog systemcatalog.SystemCatalog, resolver *logicalReplicationResolver,
	taskManager task.TaskManager,
) (eventhandlers.LogicalReplicationEventHandler, error) {

	logger, err := logging.NewLogger("TransactionTracker")
	if err != nil {
		return nil, err
	}

	tt := &transactionTracker{
		timeout:                      timeout,
		systemCatalog:                systemCatalog,
		taskManager:                  taskManager,
		relations:                    containers.NewRelationCache[*pgtypes.RelationMessage](),
		logger:                       logger,
		resolver:                     resolver,
		supportsDecompressionMarkers: replicationContext.IsTSDB212GE(),
	}

	tt.activeTransaction = &transaction{
		transactionTracker: tt,
		queue:              containers.NewQueue[*transactionEntry](int(maxSize + 1)),
	}

	return tt, nil
}
func (tt *transactionTracker) PostConstruct() error {
	tt.taskManager.RegisterReplicationEventHandler(tt)
	return nil
}

func (tt *transactionTracker) OnTableSnapshotStartedEvent(
	snapshotName string, table spicatalog.BaseTable,
) error {

	return tt.resolver.OnTableSnapshotStartedEvent(snapshotName, table)
}

func (tt *transactionTracker) OnTableSnapshotFinishedEvent(
	snapshotName string, table spicatalog.BaseTable, lsn pgtypes.LSN,
) error {

	return tt.resolver.OnTableSnapshotFinishedEvent(snapshotName, table, lsn)
}

func (tt *transactionTracker) OnSnapshottingStartedEvent(
	snapshotName string,
) error {

	return tt.resolver.OnSnapshottingStartedEvent(snapshotName)
}

func (tt *transactionTracker) OnSnapshottingFinishedEvent() error {
	return tt.resolver.OnSnapshottingFinishedEvent()
}

func (tt *transactionTracker) OnChunkSnapshotStartedEvent(
	hypertable *spicatalog.Hypertable, chunk *spicatalog.Chunk,
) error {

	return tt.resolver.OnChunkSnapshotStartedEvent(hypertable, chunk)
}

func (tt *transactionTracker) OnChunkSnapshotFinishedEvent(
	hypertable *spicatalog.Hypertable, chunk *spicatalog.Chunk, snapshot pgtypes.LSN,
) error {

	return tt.resolver.OnChunkSnapshotFinishedEvent(hypertable, chunk, snapshot)
}

func (tt *transactionTracker) OnRelationEvent(
	xld pgtypes.XLogData, msg *pgtypes.RelationMessage,
) error {

	tt.relations.Set(msg.RelationID, msg)
	return tt.resolver.OnRelationEvent(xld, msg)
}

func (tt *transactionTracker) OnBeginEvent(
	xld pgtypes.XLogData, msg *pgtypes.BeginMessage,
) error {

	tt.startTransaction(msg.Xid, msg.CommitTime, pgtypes.LSN(msg.FinalLSN))
	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnBeginEvent(xld, msg)
	}
	return nil
}

func (tt *transactionTracker) OnCommitEvent(
	xld pgtypes.XLogData, msg *pgtypes.CommitMessage,
) error {

	// There isn't a running transaction, which can happen when we
	// got restarted and the last processed LSN was inside a running
	// transaction. In this case we skip all earlier logrepl messages
	// and keep going from where we left off.
	if !tt.activeTransaction.active {
		return tt.resolver.OnCommitEvent(xld, msg)
	}
	tt.activeTransaction.active = false

	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnCommitEvent(xld, msg)
	}

	if tt.activeTransaction.compressionUpdate != nil {
		message := tt.activeTransaction.compressionUpdate
		chunkId := message.msg.(*pgtypes.UpdateMessage).NewValues["id"].(int32)
		if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
			if err := tt.resolver.onChunkCompressionEvent(xld, chunk); err != nil {
				return err
			}
			if err := tt.resolver.onChunkUpdateEvent(xld, message.msg.(*pgtypes.UpdateMessage)); err != nil {
				return err
			}
		}

		// If there isn't a decompression event in the same transaction where done here
		if tt.activeTransaction.decompressionUpdate == nil {
			return nil
		}
	}

	if tt.activeTransaction.decompressionUpdate != nil {
		message := tt.activeTransaction.decompressionUpdate
		chunkId := message.msg.(*pgtypes.UpdateMessage).NewValues["id"].(int32)
		if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
			if err := tt.resolver.onChunkDecompressionEvent(xld, chunk); err != nil {
				return err
			}
			return tt.resolver.onChunkUpdateEvent(xld, message.msg.(*pgtypes.UpdateMessage))
		}
	}

	if err := tt.activeTransaction.drain(); err != nil {
		return err
	}
	return tt.resolver.OnCommitEvent(xld, msg)
}

func (tt *transactionTracker) OnInsertEvent(
	xld pgtypes.XLogData, msg *pgtypes.InsertMessage,
) error {

	relation, present := tt.relations.Get(msg.RelationID)
	if present {
		// If no insert events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genHypertableInsertEvent &&
			!spicatalog.IsHypertableEvent(relation) &&
			!spicatalog.IsChunkEvent(relation) {

			return nil
		}
	}

	if tt.activeTransaction.active {
		// If we already know that the transaction represents a decompression in TimescaleDB
		// we can start to discard all newly incoming INSERTs immediately, since those are the
		// re-inserted, uncompressed rows that were already replicated into events in the past.
		if (tt.activeTransaction.decompressionUpdate != nil ||
			tt.activeTransaction.ongoingDecompression) &&
			!spicatalog.IsHypertableEvent(relation) &&
			!spicatalog.IsChunkEvent(relation) {

			return nil
		}

		if !tt.supportsDecompressionMarkers {
			handled, err := tt.activeTransaction.pushTransactionEntry(&transactionEntry{
				xld: xld,
				msg: msg,
			})
			if err != nil {
				return err
			} else if handled {
				return nil
			}
		}
	}

	return tt.resolver.OnInsertEvent(xld, msg)
}

func (tt *transactionTracker) OnUpdateEvent(
	xld pgtypes.XLogData, msg *pgtypes.UpdateMessage,
) error {

	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnUpdateEvent(xld, msg)
	}

	updateEntry := &transactionEntry{
		xld: xld,
		msg: msg,
	}

	if relation, present := tt.relations.Get(msg.RelationID); present {
		if spicatalog.IsChunkEvent(relation) {
			chunkId := msg.NewValues["id"].(int32)
			if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
				oldChunkStatus := chunk.Status()
				newChunkStatus := msg.NewValues["status"].(int32)

				// If true, we found a compression event
				if oldChunkStatus == 0 && newChunkStatus != 0 {
					tt.activeTransaction.compressionUpdate = updateEntry
				} else if tt.activeTransaction.compressionUpdate != nil {
					compressionMsg := tt.activeTransaction.compressionUpdate.msg.(*pgtypes.UpdateMessage)
					if compressionMsg.RelationID == msg.RelationID {
						oldChunkStatus = compressionMsg.NewValues["status"].(int32)
					}
				}

				previouslyCompressed := oldChunkStatus != 0 && newChunkStatus == 0
				tt.logger.Verbosef("Chunk %d: status=%d, new value=%d", chunkId, oldChunkStatus, newChunkStatus)

				if previouslyCompressed {
					tt.activeTransaction.decompressionUpdate = updateEntry
				}
			}
		}

		// If no update events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genHypertableUpdateEvent &&
			!spicatalog.IsHypertableEvent(relation) {

			return nil
		}
	}

	if tt.activeTransaction.active {
		handled, err := tt.activeTransaction.pushTransactionEntry(&transactionEntry{
			xld: xld,
			msg: msg,
		})
		if err != nil {
			return err
		} else if handled {
			return nil
		}
	}

	return tt.resolver.OnUpdateEvent(xld, msg)
}

func (tt *transactionTracker) OnDeleteEvent(
	xld pgtypes.XLogData, msg *pgtypes.DeleteMessage,
) error {

	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnDeleteEvent(xld, msg)
	}

	if relation, present := tt.relations.Get(msg.RelationID); present {
		// If no delete events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genHypertableDeleteEvent &&
			!spicatalog.IsHypertableEvent(relation) &&
			!spicatalog.IsChunkEvent(relation) {

			return nil
		}
	}

	if tt.activeTransaction.active {
		handled, err := tt.activeTransaction.pushTransactionEntry(&transactionEntry{
			xld: xld,
			msg: msg,
		})
		if err != nil {
			return err
		} else if handled {
			return nil
		}
	}

	return tt.resolver.OnDeleteEvent(xld, msg)
}

func (tt *transactionTracker) OnTruncateEvent(
	xld pgtypes.XLogData, msg *pgtypes.TruncateMessage,
) error {

	// Since internal catalog tables shouldn't EVER be truncated, we ignore this case
	// and only collect the truncate event if we expect the event to be generated in
	// the later step. If no event is going to be created we discard it right here
	// and now.
	if !tt.resolver.genHypertableTruncateEvent {
		return nil
	}

	if !tt.supportsDecompressionMarkers && tt.activeTransaction.active {
		handled, err := tt.activeTransaction.pushTransactionEntry(&transactionEntry{
			xld: xld,
			msg: msg,
		})
		if err != nil {
			return err
		} else if handled {
			return nil
		}
	}

	return tt.resolver.OnTruncateEvent(xld, msg)
}

func (tt *transactionTracker) OnTypeEvent(
	xld pgtypes.XLogData, msg *pgtypes.TypeMessage,
) error {

	return tt.resolver.OnTypeEvent(xld, msg)
}

func (tt *transactionTracker) OnOriginEvent(
	xld pgtypes.XLogData, msg *pgtypes.OriginMessage,
) error {

	return tt.resolver.OnOriginEvent(xld, msg)
}

func (tt *transactionTracker) OnMessageEvent(
	xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage,
) error {

	// If the message is transactional we need to store it into the currently collected
	// transaction, otherwise we can run it straight away.
	if msg.IsTransactional() {
		if msg.Prefix == decompressionMarkerStartId {
			tt.activeTransaction.ongoingDecompression = true
			return nil
		} else if msg.Prefix == decompressionMarkerEndId &&
			(tt.activeTransaction.active && tt.activeTransaction.ongoingDecompression) {
			tt.activeTransaction.ongoingDecompression = false
			return nil
		}

		// If we don't want to generate the message events later one, we'll discard it
		// right here and now instead of collecting it for later.
		if !tt.resolver.genMessageEvent {
			return nil
		}

		if !tt.supportsDecompressionMarkers {
			if tt.activeTransaction.active {
				handled, err := tt.activeTransaction.pushTransactionEntry(&transactionEntry{
					xld: xld,
					msg: msg,
				})
				if err != nil {
					return err
				} else if handled {
					return nil
				}
			}
		}
	}

	return tt.resolver.OnMessageEvent(xld, msg)
}

func (tt *transactionTracker) startTransaction(
	xid uint32, commitTime time.Time, finalLSN pgtypes.LSN,
) {

	for {
		// Make sure the queue is fully drained by this time
		if v := tt.activeTransaction.queue.Pop(); v == nil {
			break
		}
	}

	tt.activeTransaction = &transaction{
		transactionTracker: tt,
		maxSize:            tt.activeTransaction.maxSize,
		xid:                xid,
		finalLSN:           finalLSN,
		commitTime:         commitTime,
		deadline:           time.Now().Add(tt.timeout),
		queue:              tt.activeTransaction.queue,
	}
}

type transaction struct {
	active               bool
	transactionTracker   *transactionTracker
	maxSize              uint
	deadline             time.Time
	xid                  uint32
	commitTime           time.Time
	finalLSN             pgtypes.LSN
	queue                *containers.Queue[*transactionEntry]
	queueLength          uint
	compressionUpdate    *transactionEntry
	decompressionUpdate  *transactionEntry
	overflowed           bool
	timedOut             bool
	ongoingDecompression bool
}

func (t *transaction) pushTransactionEntry(
	entry *transactionEntry,
) (bool, error) {

	if t.timedOut || t.overflowed {
		return false, nil
	}

	t.queue.Push(entry)
	t.queueLength++

	if t.deadline.Before(time.Now()) {
		t.timedOut = true
	}

	if t.queueLength == t.maxSize {
		t.overflowed = true
	}

	if t.timedOut || t.overflowed {
		return true, t.drain()
	}

	return true, nil
}

func (t *transaction) drain() error {
	for {
		entry := t.queue.Pop()
		if entry == nil {
			break
		}

		switch msg := entry.msg.(type) {
		case *pgtypes.BeginMessage:
			if err := t.transactionTracker.resolver.OnBeginEvent(entry.xld, msg); err != nil {
				return err
			}
		case *pgtypes.InsertMessage:
			if err := t.transactionTracker.resolver.OnInsertEvent(entry.xld, msg); err != nil {
				return err
			}
		case *pgtypes.UpdateMessage:
			if err := t.transactionTracker.resolver.OnUpdateEvent(entry.xld, msg); err != nil {
				return err
			}
		case *pgtypes.DeleteMessage:
			if err := t.transactionTracker.resolver.OnDeleteEvent(entry.xld, msg); err != nil {
				return err
			}
		case *pgtypes.TruncateMessage:
			if err := t.transactionTracker.resolver.OnTruncateEvent(entry.xld, msg); err != nil {
				return err
			}
		case *pgtypes.LogicalReplicationMessage:
			if err := t.transactionTracker.resolver.OnMessageEvent(entry.xld, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

type transactionEntry struct {
	xld pgtypes.XLogData
	msg pglogrepl.Message
}
