package logicalreplicationresolver

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventhandler"
	"github.com/noctarius/timescaledb-event-streamer/internal/pg/decoding"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/model"
	"time"
)

const (
	decompressionMarkerStartId = "::timescaledb-decompression-start"
	decompressionMarkerEndId   = "::timescaledb-decompression-end"
)

type transactionTracker struct {
	timeout            time.Duration
	maxSize            uint
	relations          map[uint32]*decoding.RelationMessage
	resolver           *logicalReplicationResolver
	systemCatalog      *systemcatalog.SystemCatalog
	currentTransaction *transaction
}

func newTransactionTracker(timeout time.Duration, maxSize uint, config *sysconfig.SystemConfig,
	dispatcher *eventhandler.Dispatcher, systemCatalog *systemcatalog.SystemCatalog) eventhandler.LogicalReplicationEventHandler {

	return &transactionTracker{
		timeout:       timeout,
		maxSize:       maxSize,
		systemCatalog: systemCatalog,
		relations:     make(map[uint32]*decoding.RelationMessage),
		resolver:      newLogicalReplicationResolver(config, dispatcher, systemCatalog),
	}
}

func (tt *transactionTracker) OnChunkSnapshotStartedEvent(hypertable *model.Hypertable, chunk *model.Chunk) error {
	return tt.resolver.OnChunkSnapshotStartedEvent(hypertable, chunk)
}

func (tt *transactionTracker) OnChunkSnapshotFinishedEvent(
	hypertable *model.Hypertable, chunk *model.Chunk, snapshot pglogrepl.LSN) error {

	return tt.resolver.OnChunkSnapshotFinishedEvent(hypertable, chunk, snapshot)
}

func (tt *transactionTracker) OnRelationEvent(xld pglogrepl.XLogData, msg *decoding.RelationMessage) error {
	tt.relations[msg.RelationID] = msg
	return tt.resolver.OnRelationEvent(xld, msg)
}

func (tt *transactionTracker) OnBeginEvent(xld pglogrepl.XLogData, msg *decoding.BeginMessage) error {
	tt.currentTransaction = tt.newTransaction(msg.Xid, msg.CommitTime, msg.FinalLSN)
	return nil
}

func (tt *transactionTracker) OnCommitEvent(xld pglogrepl.XLogData, msg *decoding.CommitMessage) error {
	currentTransaction := tt.currentTransaction
	tt.currentTransaction = nil

	currentTransaction.queue.Lock()

	if currentTransaction.compressionUpdate != nil {
		message := currentTransaction.compressionUpdate
		chunkId := message.msg.(*decoding.UpdateMessage).NewValues["id"].(int32)
		if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
			if err := tt.resolver.onChunkCompressionEvent(xld, chunk); err != nil {
				return err
			}
			if err := tt.resolver.onChunkUpdateEvent(message.msg.(*decoding.UpdateMessage)); err != nil {
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
		chunkId := message.msg.(*decoding.UpdateMessage).NewValues["id"].(int32)
		if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
			if err := tt.resolver.onChunkDecompressionEvent(xld, chunk); err != nil {
				return err
			}
			return tt.resolver.onChunkUpdateEvent(message.msg.(*decoding.UpdateMessage))
		}
	}

	if err := currentTransaction.drain(); err != nil {
		return err
	}
	return tt.resolver.OnCommitEvent(xld, msg)
}

func (tt *transactionTracker) OnInsertEvent(xld pglogrepl.XLogData, msg *decoding.InsertMessage) error {
	relation, ok := tt.relations[msg.RelationID]
	if ok {
		// If no insert events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genInsertEvent &&
			!model.IsHypertableEvent(relation) &&
			!model.IsChunkEvent(relation) {

			return nil
		}
	}

	if tt.currentTransaction != nil {
		// If we already know that the transaction represents a decompression in TimescaleDB
		// we can start to discard all newly incoming INSERTs immediately, since those are the
		// re-inserted, uncompressed rows that were already replicated into events in the past.
		if (tt.currentTransaction.decompressionUpdate != nil ||
			tt.currentTransaction.ongoingDecompression) &&
			!model.IsHypertableEvent(relation) &&
			!model.IsChunkEvent(relation) {

			return nil
		}

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

	return tt.resolver.OnInsertEvent(xld, msg)
}

func (tt *transactionTracker) OnUpdateEvent(xld pglogrepl.XLogData, msg *decoding.UpdateMessage) error {
	updateEntry := &transactionEntry{
		xld: xld,
		msg: msg,
	}

	if relation, ok := tt.relations[msg.RelationID]; ok {
		if model.IsChunkEvent(relation) {
			chunkId := msg.NewValues["id"].(int32)
			if chunk, present := tt.systemCatalog.FindChunkById(chunkId); present {
				oldChunkStatus := chunk.Status()
				newChunkStatus := msg.NewValues["status"].(int32)

				// If true, we found a compression event
				if oldChunkStatus == 0 && newChunkStatus != 0 {
					tt.currentTransaction.compressionUpdate = updateEntry
				} else if tt.currentTransaction.compressionUpdate != nil {
					compressionMsg := tt.currentTransaction.compressionUpdate.msg.(*decoding.UpdateMessage)
					if compressionMsg.RelationID == msg.RelationID {
						oldChunkStatus = compressionMsg.NewValues["status"].(int32)
					}
				}

				previouslyCompressed := oldChunkStatus != 0 && newChunkStatus == 0
				logger.Verbosef("Chunk %d: status=%d, new value=%d", chunkId, oldChunkStatus, newChunkStatus)

				if previouslyCompressed {
					tt.currentTransaction.decompressionUpdate = updateEntry
				}
			}
		}

		// If no update events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genUpdateEvent &&
			!model.IsHypertableEvent(relation) {

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

func (tt *transactionTracker) OnDeleteEvent(xld pglogrepl.XLogData, msg *decoding.DeleteMessage) error {
	if relation, ok := tt.relations[msg.RelationID]; ok {
		// If no delete events are going to be generated, and we don't need to update the catalog,
		// we can already ignore the event here and prevent it from hogging memory while we wait
		// for the transaction to be completely transmitted
		if !tt.resolver.genDeleteEvent &&
			!model.IsHypertableEvent(relation) &&
			!model.IsChunkEvent(relation) {

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

func (tt *transactionTracker) OnTruncateEvent(xld pglogrepl.XLogData, msg *decoding.TruncateMessage) error {
	// Since internal catalog tables shouldn't EVER be truncated, we ignore this case
	// and only collect the truncate event if we expect the event to be generated in
	// the later step. If no event is going to be created we discard it right here
	// and now.
	if !tt.resolver.genTruncateEvent {
		return nil
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

	return tt.resolver.OnTruncateEvent(xld, msg)
}

func (tt *transactionTracker) OnTypeEvent(xld pglogrepl.XLogData, msg *decoding.TypeMessage) error {
	return tt.resolver.OnTypeEvent(xld, msg)
}

func (tt *transactionTracker) OnOriginEvent(xld pglogrepl.XLogData, msg *decoding.OriginMessage) error {
	return tt.resolver.OnOriginEvent(xld, msg)
}

func (tt *transactionTracker) OnMessageEvent(xld pglogrepl.XLogData, msg *decoding.LogicalReplicationMessage) error {
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

	return tt.resolver.OnMessageEvent(xld, msg)
}

func (tt *transactionTracker) newTransaction(xid uint32, commitTime time.Time, finalLSN pglogrepl.LSN) *transaction {
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
	finalLSN             pglogrepl.LSN
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
		case *decoding.BeginMessage:
			if err := t.transactionTracker.resolver.OnBeginEvent(entry.xld, msg); err != nil {
				return err
			}
		case *decoding.InsertMessage:
			if err := t.transactionTracker.resolver.OnInsertEvent(entry.xld, msg); err != nil {
				return err
			}
		case *decoding.UpdateMessage:
			if err := t.transactionTracker.resolver.OnUpdateEvent(entry.xld, msg); err != nil {
				return err
			}
		case *decoding.DeleteMessage:
			if err := t.transactionTracker.resolver.OnDeleteEvent(entry.xld, msg); err != nil {
				return err
			}
		case *decoding.TruncateMessage:
			if err := t.transactionTracker.resolver.OnTruncateEvent(entry.xld, msg); err != nil {
				return err
			}
		case *decoding.LogicalReplicationMessage:
			if err := t.transactionTracker.resolver.OnMessageEvent(entry.xld, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

type transactionEntry struct {
	xld pglogrepl.XLogData
	msg pglogrepl.Message
}
