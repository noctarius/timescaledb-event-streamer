package transactional

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/pg/decoding"
)

type TransactionMonitor struct {
	currentLSN    pglogrepl.LSN
	transactionId uint32
}

func NewTransactionMonitor() *TransactionMonitor {
	return &TransactionMonitor{}
}

func (t *TransactionMonitor) TransactionId() uint32 {
	return t.transactionId
}

func (t *TransactionMonitor) OnRelationEvent(_ pglogrepl.XLogData, _ *decoding.RelationMessage) error {
	return nil
}

func (t *TransactionMonitor) OnBeginEvent(_ pglogrepl.XLogData, msg *decoding.BeginMessage) error {
	t.currentLSN = msg.FinalLSN
	t.transactionId = msg.Xid
	return nil
}

func (t *TransactionMonitor) OnCommitEvent(_ pglogrepl.XLogData, msg *decoding.CommitMessage) error {
	t.currentLSN = msg.CommitLSN
	return nil
}

func (t *TransactionMonitor) OnInsertEvent(_ pglogrepl.XLogData, _ *decoding.InsertMessage) error {
	return nil
}

func (t *TransactionMonitor) OnUpdateEvent(_ pglogrepl.XLogData, _ *decoding.UpdateMessage) error {
	return nil
}

func (t *TransactionMonitor) OnDeleteEvent(_ pglogrepl.XLogData, _ *decoding.DeleteMessage) error {
	return nil
}

func (t *TransactionMonitor) OnTruncateEvent(_ pglogrepl.XLogData, _ *decoding.TruncateMessage) error {
	return nil
}

func (t *TransactionMonitor) OnTypeEvent(_ pglogrepl.XLogData, _ *decoding.TypeMessage) error {
	return nil
}

func (t *TransactionMonitor) OnOriginEvent(_ pglogrepl.XLogData, _ *decoding.OriginMessage) error {
	return nil
}
