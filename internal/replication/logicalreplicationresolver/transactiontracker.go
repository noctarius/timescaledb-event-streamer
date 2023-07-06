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
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	spicatalog "github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"time"
)

const (
	decompressionMarkerStartId = "::timescaledb-decompression-start"
	decompressionMarkerEndId   = "::timescaledb-decompression-end"
)

type transactionTracker struct {
	timeout                      time.Duration
	maxSize                      uint
	relations                    map[uint32]*pgtypes.RelationMessage
	resolver                     *logicalReplicationResolver
	systemCatalog                *systemcatalog.SystemCatalog
	currentTransaction           *transaction
	logger                       *logging.Logger
	supportsDecompressionMarkers bool
}

func newTransactionTracker(timeout time.Duration, maxSize uint, replicationContext *context.ReplicationContext,
	systemCatalog *systemcatalog.SystemCatalog, resolver *logicalReplicationResolver,
) (eventhandlers.LogicalReplicationEventHandler, error) {

	logger, err := logging.NewLogger("TransactionTracker")
	if err != nil {
		return nil, err
	}

	return &transactionTracker{
		timeout:                      timeout,
		maxSize:                      maxSize,
		systemCatalog:                systemCatalog,
		relations:                    make(map[uint32]*pgtypes.RelationMessage),
		logger:                       logger,
		resolver:                     resolver,
		supportsDecompressionMarkers: replicationContext.IsTSDB212GE(),
	}, nil
}

func (tt *transactionTracker) OnHypertableSnapshotStartedEvent(hypertable *spicatalog.Hypertable) error {
	return tt.resolver.OnHypertableSnapshotStartedEvent(hypertable)
}

func (tt *transactionTracker) OnHypertableSnapshotFinishedEvent(hypertable *spicatalog.Hypertable) error {
	return tt.resolver.OnHypertableSnapshotFinishedEvent(hypertable)
}

func (tt *transactionTracker) OnSnapshottingStartedEvent(snapshotName string) error {
	return tt.resolver.OnSnapshottingStartedEvent(snapshotName)
}

func (tt *transactionTracker) OnSnapshottingFinishedEvent() error {
	return tt.resolver.OnSnapshottingFinishedEvent()
}

func (tt *transactionTracker) OnChunkSnapshotStartedEvent(
	hypertable *spicatalog.Hypertable, chunk *spicatalog.Chunk) error {

	return tt.resolver.OnChunkSnapshotStartedEvent(hypertable, chunk)
}

func (tt *transactionTracker) OnChunkSnapshotFinishedEvent(
	hypertable *spicatalog.Hypertable, chunk *spicatalog.Chunk, snapshot pgtypes.LSN) error {

	return tt.resolver.OnChunkSnapshotFinishedEvent(hypertable, chunk, snapshot)
}

func (tt *transactionTracker) OnRelationEvent(xld pgtypes.XLogData, msg *pgtypes.RelationMessage) error {
	tt.relations[msg.RelationID] = msg
	return tt.resolver.OnRelationEvent(xld, msg)
}

func (tt *transactionTracker) OnBeginEvent(xld pgtypes.XLogData, msg *pgtypes.BeginMessage) error {
	tt.currentTransaction = tt.newTransaction(msg.Xid, msg.CommitTime, pgtypes.LSN(msg.FinalLSN))
	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnBeginEvent(xld, msg)
	}
	return nil
}

func (tt *transactionTracker) OnCommitEvent(xld pgtypes.XLogData, msg *pgtypes.CommitMessage) error {
	// There isn't a running transaction, which can happen when we
	// got restarted and the last processed LSN was inside a running
	// transaction. In this case we skip all earlier logrepl messages
	// and keep going from where we left off.
	if tt.currentTransaction == nil {
		return tt.resolver.OnCommitEvent(xld, msg)
	}

	currentTransaction := tt.currentTransaction
	tt.currentTransaction = nil

	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnCommitEvent(xld, msg)
	}

	currentTransaction.queue.Lock()

	if currentTransaction.compressionUpdate != nil {
		message := currentTransaction.compressionUpdate
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
		if currentTransaction.decompressionUpdate == nil {
			return nil
		}
	}

	if currentTransaction.decompressionUpdate != nil {
		message := currentTransaction.decompressionUpdate
		chunkId := message.msg.(*pgtypes.UpdateMessage).NewValues["id"].(int32)
		if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
			if err := tt.resolver.onChunkDecompressionEvent(xld, chunk); err != nil {
				return err
			}
			return tt.resolver.onChunkUpdateEvent(xld, message.msg.(*pgtypes.UpdateMessage))
		}
	}

	if err := currentTransaction.drain(); err != nil {
		return err
	}
	return tt.resolver.OnCommitEvent(xld, msg)
}

func (tt *transactionTracker) OnInsertEvent(xld pgtypes.XLogData, msg *pgtypes.InsertMessage) error {
	relation, ok := tt.relations[msg.RelationID]
	if ok {
		// If no insert events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genInsertEvent &&
			!spicatalog.IsHypertableEvent(relation) &&
			!spicatalog.IsChunkEvent(relation) {

			return nil
		}
	}

	if tt.currentTransaction != nil {
		// If we already know that the transaction represents a decompression in TimescaleDB
		// we can start to discard all newly incoming INSERTs immediately, since those are the
		// re-inserted, uncompressed rows that were already replicated into events in the past.
		if (tt.currentTransaction.decompressionUpdate != nil ||
			tt.currentTransaction.ongoingDecompression) &&
			!spicatalog.IsHypertableEvent(relation) &&
			!spicatalog.IsChunkEvent(relation) {

			return nil
		}

		if !tt.supportsDecompressionMarkers {
			handled, err := tt.currentTransaction.pushTransactionEntry(&transactionEntry{
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

func (tt *transactionTracker) OnUpdateEvent(xld pgtypes.XLogData, msg *pgtypes.UpdateMessage) error {
	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnUpdateEvent(xld, msg)
	}

	updateEntry := &transactionEntry{
		xld: xld,
		msg: msg,
	}

	if relation, ok := tt.relations[msg.RelationID]; ok {
		if spicatalog.IsChunkEvent(relation) {
			chunkId := msg.NewValues["id"].(int32)
			if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
				oldChunkStatus := chunk.Status()
				newChunkStatus := msg.NewValues["status"].(int32)

				// If true, we found a compression event
				if oldChunkStatus == 0 && newChunkStatus != 0 {
					tt.currentTransaction.compressionUpdate = updateEntry
				} else if tt.currentTransaction.compressionUpdate != nil {
					compressionMsg := tt.currentTransaction.compressionUpdate.msg.(*pgtypes.UpdateMessage)
					if compressionMsg.RelationID == msg.RelationID {
						oldChunkStatus = compressionMsg.NewValues["status"].(int32)
					}
				}

				previouslyCompressed := oldChunkStatus != 0 && newChunkStatus == 0
				tt.logger.Verbosef("Chunk %d: status=%d, new value=%d", chunkId, oldChunkStatus, newChunkStatus)

				if previouslyCompressed {
					tt.currentTransaction.decompressionUpdate = updateEntry
				}
			}
		}

		// If no update events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genUpdateEvent &&
			!spicatalog.IsHypertableEvent(relation) {

			return nil
		}
	}

	if tt.currentTransaction != nil {
		handled, err := tt.currentTransaction.pushTransactionEntry(&transactionEntry{
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

func (tt *transactionTracker) OnDeleteEvent(xld pgtypes.XLogData, msg *pgtypes.DeleteMessage) error {
	if tt.supportsDecompressionMarkers {
		return tt.resolver.OnDeleteEvent(xld, msg)
	}

	if relation, ok := tt.relations[msg.RelationID]; ok {
		// If no delete events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genDeleteEvent &&
			!spicatalog.IsHypertableEvent(relation) &&
			!spicatalog.IsChunkEvent(relation) {

			return nil
		}
	}

	if tt.currentTransaction != nil {
		handled, err := tt.currentTransaction.pushTransactionEntry(&transactionEntry{
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

func (tt *transactionTracker) OnTruncateEvent(xld pgtypes.XLogData, msg *pgtypes.TruncateMessage) error {
	// Since internal catalog tables shouldn't EVER be truncated, we ignore this case
	// and only collect the truncate event if we expect the event to be generated in
	// the later step. If no event is going to be created we discard it right here
	// and now.
	if !tt.resolver.genTruncateEvent {
		return nil
	}

	if !tt.supportsDecompressionMarkers && tt.currentTransaction != nil {
		handled, err := tt.currentTransaction.pushTransactionEntry(&transactionEntry{
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

func (tt *transactionTracker) OnTypeEvent(xld pgtypes.XLogData, msg *pgtypes.TypeMessage) error {
	return tt.resolver.OnTypeEvent(xld, msg)
}

func (tt *transactionTracker) OnOriginEvent(xld pgtypes.XLogData, msg *pgtypes.OriginMessage) error {
	return tt.resolver.OnOriginEvent(xld, msg)
}

func (tt *transactionTracker) OnMessageEvent(xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage) error {
	// If the message is transactional we need to store it into the currently collected
	// transaction, otherwise we can run it straight away.
	if msg.IsTransactional() {
		if msg.Prefix == decompressionMarkerStartId {
			tt.currentTransaction.ongoingDecompression = true
			return nil
		} else if msg.Prefix == decompressionMarkerEndId &&
			(tt.currentTransaction != nil && tt.currentTransaction.ongoingDecompression) {
			tt.currentTransaction.ongoingDecompression = false
			return nil
		}

		// If we don't want to generate the message events later one, we'll discard it
		// right here and now instead of collecting it for later.
		if !tt.resolver.genMessageEvent {
			return nil
		}

		if !tt.supportsDecompressionMarkers {
			if tt.currentTransaction != nil {
				handled, err := tt.currentTransaction.pushTransactionEntry(&transactionEntry{
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

func (tt *transactionTracker) newTransaction(xid uint32, commitTime time.Time, finalLSN pgtypes.LSN) *transaction {
	return &transaction{
		transactionTracker: tt,
		xid:                xid,
		commitTime:         commitTime,
		finalLSN:           finalLSN,
		queue:              supporting.NewQueue[*transactionEntry](),
		maxSize:            tt.maxSize,
		deadline:           time.Now().Add(tt.timeout),
	}
}

type transaction struct {
	transactionTracker   *transactionTracker
	maxSize              uint
	deadline             time.Time
	xid                  uint32
	commitTime           time.Time
	finalLSN             pgtypes.LSN
	queue                *supporting.Queue[*transactionEntry]
	queueLength          uint
	compressionUpdate    *transactionEntry
	decompressionUpdate  *transactionEntry
	overflowed           bool
	timedOut             bool
	ongoingDecompression bool
}

func (t *transaction) pushTransactionEntry(entry *transactionEntry) (bool, error) {
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
