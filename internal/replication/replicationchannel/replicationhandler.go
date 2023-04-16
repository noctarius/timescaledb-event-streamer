package replicationchannel

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/noctarius/timescaledb-event-streamer/internal/pgdecoding"
	repcontext "github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"runtime"
	"time"
)

type replicationHandler struct {
	replicationContext *repcontext.ReplicationContext
	clientXLogPos      pglogrepl.LSN
	relations          map[uint32]*pglogrepl.RelationMessage
	shutdownAwaiter    *supporting.ShutdownAwaiter
	lastTransactionId  *uint32
	logger             *logging.Logger
}

func newReplicationHandler(replicationContext *repcontext.ReplicationContext) (*replicationHandler, error) {
	logger, err := logging.NewLogger("ReplicationHandler")
	if err != nil {
		return nil, err
	}

	return &replicationHandler{
		replicationContext: replicationContext,
		relations:          make(map[uint32]*pglogrepl.RelationMessage, 0),
		shutdownAwaiter:    supporting.NewShutdownAwaiter(),
		logger:             logger,
	}, nil
}

func (rh *replicationHandler) stopReplicationHandler() error {
	rh.logger.Println("Starting to shutdown")
	rh.shutdownAwaiter.SignalShutdown()
	return rh.shutdownAwaiter.AwaitDone()
}

func (rh *replicationHandler) startReplicationHandler(replicationConnection *repcontext.ReplicationConnection) error {
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	runtime.LockOSThread()
	for {
		select {
		case <-rh.shutdownAwaiter.AwaitShutdownChan():
			runtime.UnlockOSThread()
			rh.shutdownAwaiter.SignalDone()
			return nil
		default:
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			if err := replicationConnection.SendStatusUpdate(); err != nil {
				rh.logger.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		rawMsg, err := replicationConnection.ReceiveMessage(nextStandbyMessageDeadline)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		// Timeout reached, we'll just ignore that though :)
		if rawMsg == nil {
			continue
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			rh.logger.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			rh.logger.Warnf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				rh.logger.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			//logger.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				rh.logger.Fatalln("ParseXLogData failed:", err)
			}
			if err := rh.handleXLogData(xld); err != nil {
				return errors.Wrap(err, 0)
			}
			rh.replicationContext.AcknowledgeReceived(xld)
		}
	}
}

func (rh *replicationHandler) handleXLogData(xld pglogrepl.XLogData) error {
	msg, err := pgdecoding.ParseXlogData(xld.WALData, rh.lastTransactionId)
	if err != nil {
		return fmt.Errorf("parsing logical replication message: %s", err)
	}

	if err := rh.handleReplicationEvents(xld, msg); err != nil {
		rh.logger.Warnf("handling replication event message failed: %s => %+v", err, msg)
	}

	rh.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	// Free memory early (due to potential queuing of records)
	xld.WALData = nil

	return nil
}

func (rh *replicationHandler) handleReplicationEvents(xld pglogrepl.XLogData, msg pglogrepl.Message) error {
	rh.logger.Debugf("EVENT: %+v", msg)
	switch logicalMsg := msg.(type) {
	case *pglogrepl.RelationMessage:
		rh.relations[logicalMsg.RelationID] = logicalMsg
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyBaseReplicationEventHandler(
				func(handler eventhandlers.BaseReplicationEventHandler) error {
					return handler.OnRelationEvent(xld, (*pgtypes.RelationMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.BeginMessage:
		rh.lastTransactionId = &logicalMsg.Xid
		// Indicates the beginning of a group of changes in a transaction. This is only
		// sent for committed transactions. You won't get any events from rolled back
		// transactions.
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnBeginEvent(xld, (*pgtypes.BeginMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.CommitMessage:
		rh.lastTransactionId = nil
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnCommitEvent(xld, (*pgtypes.CommitMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.InsertMessage:
		return rh.handleInsertMessage(xld, logicalMsg)
	case *pglogrepl.UpdateMessage:
		return rh.handleUpdateMessage(xld, logicalMsg)
	case *pglogrepl.DeleteMessage:
		return rh.handleDeleteMessage(xld, logicalMsg)
	case *pglogrepl.TruncateMessage:
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnTruncateEvent(xld, (*pgtypes.TruncateMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.TypeMessage:
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnTypeEvent(xld, (*pgtypes.TypeMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.OriginMessage:
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnOriginEvent(xld, (*pgtypes.OriginMessage)(logicalMsg))
				},
			)
		})
	case *pgtypes.LogicalReplicationMessage:
		return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnMessageEvent(xld, logicalMsg)
				},
			)
		})
	default:
		return fmt.Errorf("unknown message type in pgoutput stream: %T", logicalMsg)
	}
}

func (rh *replicationHandler) handleDeleteMessage(xld pglogrepl.XLogData, msg *pglogrepl.DeleteMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		rh.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode values and remove source
	oldValues := rh.decodeValues(rel, msg.OldTuple)
	msg.OldTuple = nil

	// Adapt the message object
	internalMsg := &pgtypes.DeleteMessage{
		DeleteMessage: msg,
		OldValues:     oldValues,
	}

	return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandlers.LogicalReplicationEventHandler) error {
				return handler.OnDeleteEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) handleUpdateMessage(xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		rh.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode values and remove source
	oldValues := rh.decodeValues(rel, msg.OldTuple)
	newValues := rh.decodeValues(rel, msg.NewTuple)
	msg.OldTuple = nil
	msg.NewTuple = nil

	// Adapt the message object
	internalMsg := &pgtypes.UpdateMessage{
		UpdateMessage: msg,
		OldValues:     oldValues,
		NewValues:     newValues,
	}

	return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandlers.LogicalReplicationEventHandler) error {
				return handler.OnUpdateEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) handleInsertMessage(xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		rh.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode values and remove source
	newValues := rh.decodeValues(rel, msg.Tuple)
	msg.Tuple = nil

	// Adapt the message object
	internalMsg := &pgtypes.InsertMessage{
		InsertMessage: msg,
		NewValues:     newValues,
	}

	return rh.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandlers.LogicalReplicationEventHandler) error {
				return handler.OnInsertEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) decodeValues(relation *pglogrepl.RelationMessage,
	tupleData *pglogrepl.TupleData) map[string]any {

	values := map[string]any{}
	if tupleData == nil {
		return values
	}

	for idx, col := range tupleData.Columns {
		colName := relation.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and
			// logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text (basically anything other than the two above)
			val, err := pgdecoding.DecodeTextColumn(col.Data, relation.Columns[idx].DataType)
			if err != nil {
				rh.logger.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		case 'b': // binary data
			val, err := pgdecoding.DecodeBinaryColumn(col.Data, relation.Columns[idx].DataType)
			if err != nil {
				rh.logger.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		}
	}
	return values
}
