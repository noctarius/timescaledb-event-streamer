package logicalreplicationresolver

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"time"
)

type TransactionTracker struct {
	relations          map[uint32]*pglogrepl.RelationMessage
	resolver           *logicalReplicationResolver
	systemCatalog      *systemcatalog.SystemCatalog
	currentTransaction *transaction
}

func NewTransactionTracker(config *sysconfig.SystemConfig, dispatcher *eventhandler.Dispatcher,
	systemCatalog *systemcatalog.SystemCatalog) *TransactionTracker {

	return &TransactionTracker{
		systemCatalog: systemCatalog,
		relations:     make(map[uint32]*pglogrepl.RelationMessage),
		resolver:      newLogicalReplicationResolver(config, dispatcher, systemCatalog),
	}
}

func (t *TransactionTracker) OnChunkSnapshotStartedEvent(hypertable *model.Hypertable, chunk *model.Chunk) error {
	return t.resolver.OnChunkSnapshotStartedEvent(hypertable, chunk)
}

func (t *TransactionTracker) OnChunkSnapshotFinishedEvent(
	hypertable *model.Hypertable, chunk *model.Chunk, snapshot pglogrepl.LSN) error {

	return t.resolver.OnChunkSnapshotFinishedEvent(hypertable, chunk, snapshot)
}

func (t *TransactionTracker) OnRelationEvent(xld pglogrepl.XLogData, msg *pglogrepl.RelationMessage) error {
	t.relations[msg.RelationID] = msg
	t.resolver.OnRelationEvent(xld, msg)
	return nil
}

func (t *TransactionTracker) OnBeginEvent(_ pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error {
	t.currentTransaction = &transaction{
		xid:        msg.Xid,
		commitTime: msg.CommitTime,
		finalLSN:   msg.FinalLSN,
		queue:      newReplicationQueue[*transactionEntry](),
	}
	return nil
}

func (t *TransactionTracker) OnCommitEvent(xld pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error {
	currentTransaction := t.currentTransaction
	t.currentTransaction = nil

	currentTransaction.queue.lock()

	if currentTransaction.containsDecompression != nil {
		chunkId := currentTransaction.containsDecompression.newValues["id"].(int32)
		if chunk := t.systemCatalog.FindChunkById(chunkId); chunk != nil {
			return t.resolver.onChunkDecompressionEvent(xld, chunk)
		}
	}

	for {
		entry := currentTransaction.queue.pop()
		if entry == nil {
			break
		}

		switch msg := entry.msg.(type) {
		case *pglogrepl.BeginMessage:
			if err := t.resolver.OnBeginEvent(entry.xld, msg); err != nil {
				return err
			}
		case *pglogrepl.InsertMessage:
			if err := t.resolver.OnInsertEvent(entry.xld, msg, entry.newValues); err != nil {
				return err
			}
		case *pglogrepl.UpdateMessage:
			if err := t.resolver.OnUpdateEvent(entry.xld, msg, entry.oldValues, entry.newValues); err != nil {
				return err
			}
		case *pglogrepl.DeleteMessage:
			if err := t.resolver.OnDeleteEvent(entry.xld, msg, entry.oldValues); err != nil {
				return err
			}
		case *pglogrepl.TruncateMessage:
			if err := t.resolver.OnTruncateEvent(entry.xld, msg); err != nil {
				return err
			}
		case *decoding.LogicalReplicationMessage:
			if err := t.resolver.OnMessageEvent(entry.xld, msg); err != nil {
				return err
			}
		}
	}
	return t.resolver.OnCommitEvent(xld, msg)
}

func (t *TransactionTracker) OnInsertEvent(xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage, newValues map[string]any) error {
	t.currentTransaction.queue.push(&transactionEntry{
		xld:       xld,
		msg:       msg,
		newValues: newValues,
	})
	return nil
}

func (t *TransactionTracker) OnUpdateEvent(xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage, oldValues, newValues map[string]any) error {
	updateEntry := &transactionEntry{
		xld:       xld,
		msg:       msg,
		oldValues: oldValues,
		newValues: newValues,
	}

	if relation, ok := t.relations[msg.RelationID]; ok {
		if model.IsChunkEvent(relation) {
			chunkId := newValues["id"].(int32)
			if chunk := t.systemCatalog.FindChunkById(chunkId); chunk != nil {
				if chunk.Status() != 0 && (newValues["status"].(int32)) == 0 {
					t.currentTransaction.containsDecompression = updateEntry
				}
			}
		}
	}
	t.currentTransaction.queue.push(updateEntry)
	return nil
}

func (t *TransactionTracker) OnDeleteEvent(xld pglogrepl.XLogData,
	msg *pglogrepl.DeleteMessage, oldValues map[string]any) error {

	t.currentTransaction.queue.push(&transactionEntry{
		xld:       xld,
		msg:       msg,
		oldValues: oldValues,
	})
	return nil
}

func (t *TransactionTracker) OnTruncateEvent(xld pglogrepl.XLogData, msg *pglogrepl.TruncateMessage) error {
	t.currentTransaction.queue.push(&transactionEntry{
		xld: xld,
		msg: msg,
	})
	return nil
}

func (t *TransactionTracker) OnTypeEvent(xld pglogrepl.XLogData, msg *pglogrepl.TypeMessage) error {
	return t.resolver.OnTypeEvent(xld, msg)
}

func (t *TransactionTracker) OnOriginEvent(xld pglogrepl.XLogData, msg *pglogrepl.OriginMessage) error {
	return t.resolver.OnOriginEvent(xld, msg)
}

func (t *TransactionTracker) OnMessageEvent(xld pglogrepl.XLogData, msg *decoding.LogicalReplicationMessage) error {
	if msg.IsTransactional() {
		t.currentTransaction.queue.push(&transactionEntry{
			xld: xld,
			msg: msg,
		})
	} else {
		return t.resolver.OnMessageEvent(xld, msg)
	}
	return nil
}

type transaction struct {
	containsDecompression *transactionEntry
	xid                   uint32
	commitTime            time.Time
	finalLSN              pglogrepl.LSN
	queue                 *replicationQueue[*transactionEntry]
}

type transactionEntry struct {
	xld       pglogrepl.XLogData
	msg       pglogrepl.Message
	oldValues map[string]any
	newValues map[string]any
}
