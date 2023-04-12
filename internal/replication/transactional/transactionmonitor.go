package transactional

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
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

func (t *TransactionMonitor) OnRelationEvent(_ pglogrepl.XLogData, _ *pgtypes.RelationMessage) error {
	return nil
}

func (t *TransactionMonitor) OnBeginEvent(_ pglogrepl.XLogData, msg *pgtypes.BeginMessage) error {
	t.currentLSN = msg.FinalLSN
	t.transactionId = msg.Xid
	return nil
}

func (t *TransactionMonitor) OnCommitEvent(_ pglogrepl.XLogData, msg *pgtypes.CommitMessage) error {
	t.currentLSN = msg.CommitLSN
	return nil
}

func (t *TransactionMonitor) OnInsertEvent(_ pglogrepl.XLogData, _ *pgtypes.InsertMessage) error {
	return nil
}

func (t *TransactionMonitor) OnUpdateEvent(_ pglogrepl.XLogData, _ *pgtypes.UpdateMessage) error {
	return nil
}

func (t *TransactionMonitor) OnDeleteEvent(_ pglogrepl.XLogData, _ *pgtypes.DeleteMessage) error {
	return nil
}

func (t *TransactionMonitor) OnTruncateEvent(_ pglogrepl.XLogData, _ *pgtypes.TruncateMessage) error {
	return nil
}

func (t *TransactionMonitor) OnTypeEvent(_ pglogrepl.XLogData, _ *pgtypes.TypeMessage) error {
	return nil
}

func (t *TransactionMonitor) OnOriginEvent(_ pglogrepl.XLogData, _ *pgtypes.OriginMessage) error {
	return nil
}
