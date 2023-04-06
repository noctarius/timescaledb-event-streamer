package channels

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"runtime"
	"time"
)

var logger = logging.NewLogger("ReplicationHandler")

type replicationHandler struct {
	dispatcher        *eventhandler.Dispatcher
	clientXLogPos     pglogrepl.LSN
	relations         map[uint32]*pglogrepl.RelationMessage
	shutdownAwaiter   *supporting.ShutdownAwaiter
	lastTransactionId *uint32
}

func newReplicationHandler(dispatcher *eventhandler.Dispatcher) *replicationHandler {
	return &replicationHandler{
		dispatcher:      dispatcher,
		relations:       make(map[uint32]*pglogrepl.RelationMessage, 0),
		shutdownAwaiter: supporting.NewShutdownAwaiter(),
	}
}

func (rh *replicationHandler) stopReplicationHandler() {
	logger.Println("Starting to shutdown")
	rh.shutdownAwaiter.SignalShutdown()
	rh.shutdownAwaiter.AwaitDone()
}

func (rh *replicationHandler) startReplicationHandler(connection *pgconn.PgConn,
	identification pglogrepl.IdentifySystemResult) error {

	rh.clientXLogPos = identification.XLogPos
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
			if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), connection,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: rh.clientXLogPos}); err != nil {

				logger.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		rawMsg, err := rh.receiveNextMessage(connection, nextStandbyMessageDeadline)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			logger.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			//logger.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				logger.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			//logger.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				logger.Fatalln("ParseXLogData failed:", err)
			}
			if err := rh.handleXLogData(xld); err != nil {
				return errors.Wrap(err, 0)
			}
			rh.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func (rh *replicationHandler) handleXLogData(xld pglogrepl.XLogData) error {
	msg, err := decoding.ParseXlogData(xld.WALData, rh.lastTransactionId)
	if err != nil {
		return fmt.Errorf("parsing logical replication message: %s", err)
	}

	if err := rh.handleReplicationEvents(xld, msg); err != nil {
		logger.Printf("handling replication event message failed: %s => %+v", err, msg)
	}

	rh.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	// Free memory early (due to potential queuing of records)
	xld.WALData = nil

	return nil
}

func (rh *replicationHandler) handleReplicationEvents(xld pglogrepl.XLogData, msg pglogrepl.Message) error {
	switch logicalMsg := msg.(type) {
	case *pglogrepl.RelationMessage:
		rh.relations[logicalMsg.RelationID] = logicalMsg
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyBaseReplicationEventHandler(
				func(handler eventhandler.BaseReplicationEventHandler) error {
					return handler.OnRelationEvent(xld, (*decoding.RelationMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.BeginMessage:
		rh.lastTransactionId = &logicalMsg.Xid
		// Indicates the beginning of a group of changes in a transaction. This is only
		// sent for committed transactions. You won't get any events from rolled back
		// transactions.
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandler.LogicalReplicationEventHandler) error {
					return handler.OnBeginEvent(xld, (*decoding.BeginMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.CommitMessage:
		rh.lastTransactionId = nil
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandler.LogicalReplicationEventHandler) error {
					return handler.OnCommitEvent(xld, (*decoding.CommitMessage)(logicalMsg))
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
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandler.LogicalReplicationEventHandler) error {
					return handler.OnTruncateEvent(xld, (*decoding.TruncateMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.TypeMessage:
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandler.LogicalReplicationEventHandler) error {
					return handler.OnTypeEvent(xld, (*decoding.TypeMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.OriginMessage:
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandler.LogicalReplicationEventHandler) error {
					return handler.OnOriginEvent(xld, (*decoding.OriginMessage)(logicalMsg))
				},
			)
		})
	case *decoding.LogicalReplicationMessage:
		return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandler.LogicalReplicationEventHandler) error {
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
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode values and remove source
	oldValues := rh.decodeValues(rel, msg.OldTuple)
	msg.OldTuple = nil

	// Adapt the message object
	internalMsg := &decoding.DeleteMessage{
		DeleteMessage: msg,
		OldValues:     oldValues,
	}

	return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandler.LogicalReplicationEventHandler) error {
				return handler.OnDeleteEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) handleUpdateMessage(xld pglogrepl.XLogData, msg *pglogrepl.UpdateMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode values and remove source
	oldValues := rh.decodeValues(rel, msg.OldTuple)
	newValues := rh.decodeValues(rel, msg.NewTuple)
	msg.OldTuple = nil
	msg.NewTuple = nil

	// Adapt the message object
	internalMsg := &decoding.UpdateMessage{
		UpdateMessage: msg,
		OldValues:     oldValues,
		NewValues:     newValues,
	}

	return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandler.LogicalReplicationEventHandler) error {
				return handler.OnUpdateEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) handleInsertMessage(xld pglogrepl.XLogData, msg *pglogrepl.InsertMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode values and remove source
	newValues := rh.decodeValues(rel, msg.Tuple)
	msg.Tuple = nil

	// Adapt the message object
	internalMsg := &decoding.InsertMessage{
		InsertMessage: msg,
		NewValues:     newValues,
	}

	return rh.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandler.LogicalReplicationEventHandler) error {
				return handler.OnInsertEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) receiveNextMessage(connection *pgconn.PgConn,
	nextStandbyMessageDeadline time.Time) (pgproto3.BackendMessage, error) {

	ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
	defer cancel()

	msg, err := connection.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("ReceiveMessage failed: %s", err)
	}
	return msg, nil
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
			val, err := decoding.DecodeTextColumn(col.Data, relation.Columns[idx].DataType)
			if err != nil {
				logger.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		case 'b': // binary data
			val, err := decoding.DecodeBinaryColumn(col.Data, relation.Columns[idx].DataType)
			if err != nil {
				logger.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		}
	}
	return values
}
