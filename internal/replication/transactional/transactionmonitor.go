package transactional

import "github.com/jackc/pglogrepl"

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

func (t *TransactionMonitor) OnRelationEvent(_ pglogrepl.XLogData, _ *pglogrepl.RelationMessage) error {
	return nil
}

func (t *TransactionMonitor) OnBeginEvent(_ pglogrepl.XLogData, msg *pglogrepl.BeginMessage) error {
	t.currentLSN = msg.FinalLSN
	t.transactionId = msg.Xid
	return nil
}

func (t *TransactionMonitor) OnCommitEvent(_ pglogrepl.XLogData, msg *pglogrepl.CommitMessage) error {
	t.currentLSN = msg.CommitLSN
	return nil
}

func (t *TransactionMonitor) OnInsertEvent(_ pglogrepl.XLogData, _ *pglogrepl.InsertMessage, _ map[string]any) error {
	return nil
}

func (t *TransactionMonitor) OnUpdateEvent(_ pglogrepl.XLogData, _ *pglogrepl.UpdateMessage, _, _ map[string]any) error {
	return nil
}

func (t *TransactionMonitor) OnDeleteEvent(_ pglogrepl.XLogData, _ *pglogrepl.DeleteMessage, _ map[string]any) error {
	return nil
}

func (t *TransactionMonitor) OnTruncateEvent(_ pglogrepl.XLogData, _ *pglogrepl.TruncateMessage) error {
	return nil
}

func (t *TransactionMonitor) OnTypeEvent(_ pglogrepl.XLogData, _ *pglogrepl.TypeMessage) error {
	return nil
}

func (t *TransactionMonitor) OnOriginEvent(_ pglogrepl.XLogData, _ *pglogrepl.OriginMessage) error {
	return nil
}
