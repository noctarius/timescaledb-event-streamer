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

package replicationchannel

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/noctarius/timescaledb-event-streamer/internal/pgdecoding"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"runtime"
	"sync/atomic"
	"time"
)

type replicationHandler struct {
	replicationContext context.ReplicationContext
	taskManager        context.TaskManager
	clientXLogPos      pglogrepl.LSN
	relations          map[uint32]*pgtypes.RelationMessage
	shutdownAwaiter    *supporting.ShutdownAwaiter
	loopDead           atomic.Bool
	lastTransactionId  *uint32
	logger             *logging.Logger
}

func newReplicationHandler(replicationContext context.ReplicationContext) (*replicationHandler, error) {
	logger, err := logging.NewLogger("ReplicationHandler")
	if err != nil {
		return nil, err
	}

	return &replicationHandler{
		replicationContext: replicationContext,
		taskManager:        replicationContext.TaskManager(),
		relations:          make(map[uint32]*pgtypes.RelationMessage),
		shutdownAwaiter:    supporting.NewShutdownAwaiter(),
		logger:             logger,
		loopDead:           atomic.Bool{},
	}, nil
}

func (rh *replicationHandler) stopReplicationHandler() error {
	rh.logger.Println("Starting to shutdown")
	rh.shutdownAwaiter.SignalShutdown()
	if rh.loopDead.Load() {
		return nil
	}
	return rh.shutdownAwaiter.AwaitDone()
}

func (rh *replicationHandler) startReplicationHandler(
	replicationConnection *context.ReplicationConnection, restartLSN pgtypes.LSN,
) error {

	standbyMessageTimeout := time.Second * 5
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	runtime.LockOSThread()
	rh.logger.Infof("Starting replication handler loop")
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
			runtime.UnlockOSThread()
			rh.loopDead.Store(true)
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
			rh.logger.Tracef(
				"Primary Keepalive Message => ServerWALEnd:%s ServerTime:%s ReplyRequested:%t",
				pkm.ServerWALEnd.String(), pkm.ServerTime, pkm.ReplyRequested,
			)

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				rh.logger.Fatalln("ParseXLogData failed:", err)
			}

			// Creating the extended XLogData version which keeps context of the current log row
			xid := rh.replicationContext.LastTransactionId()
			lastBegin := rh.replicationContext.LastBeginLSN()
			lastCommit := rh.replicationContext.LastCommitLSN()
			extendedXld := pgtypes.XLogData{
				XLogData:   xld,
				LastBegin:  lastBegin,
				LastCommit: lastCommit,
				Xid:        xid,
			}

			// Skip all entries that were already replicated before the streamer was shut down
			msgType := pglogrepl.MessageType(xld.WALData[0])
			if msgType != pglogrepl.MessageTypeRelation && restartLSN > pgtypes.LSN(xld.WALStart) {
				continue
			}

			if err := rh.handleXLogData(extendedXld); err != nil {
				runtime.UnlockOSThread()
				rh.loopDead.Store(true)
				return errors.Wrap(err, 0)
			}
			rh.replicationContext.AcknowledgeReceived(extendedXld)
		}
	}
}

func (rh *replicationHandler) handleXLogData(xld pgtypes.XLogData) error {
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

func (rh *replicationHandler) handleReplicationEvents(xld pgtypes.XLogData, msg pglogrepl.Message) error {
	switch logicalMsg := msg.(type) {
	case *pglogrepl.RelationMessage:
		intLogicalMsg := pgtypes.RelationMessage(*logicalMsg)
		rh.logger.Debugf("EVENT: %s", intLogicalMsg.String())
		rh.relations[logicalMsg.RelationID] = &intLogicalMsg
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyBaseReplicationEventHandler(
				func(handler eventhandlers.BaseReplicationEventHandler) error {
					return handler.OnRelationEvent(xld, (*pgtypes.RelationMessage)(logicalMsg))
				},
			)
		})
	case *pglogrepl.BeginMessage:
		intLogicalMsg := pgtypes.BeginMessage(*logicalMsg)
		rh.logger.Debugf("EVENT: %s", intLogicalMsg.String())
		rh.replicationContext.SetLastTransactionId(intLogicalMsg.Xid)
		rh.lastTransactionId = &intLogicalMsg.Xid
		xld.Xid = intLogicalMsg.Xid
		// Indicates the beginning of a group of changes in a transaction. This is only
		// sent for committed transactions. You won't get any events from rolled back
		// transactions.
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnBeginEvent(xld, &intLogicalMsg)
				},
			)
		})
	case *pglogrepl.CommitMessage:
		intLogicalMsg := pgtypes.CommitMessage(*logicalMsg)
		rh.logger.Debugf("EVENT: %s", intLogicalMsg.String())
		rh.lastTransactionId = nil
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnCommitEvent(xld, &intLogicalMsg)
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
		intLogicalMsg := pgtypes.TruncateMessage(*logicalMsg)
		rh.logger.Debugf("EVENT: %s", intLogicalMsg.String())
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnTruncateEvent(xld, &intLogicalMsg)
				},
			)
		})
	case *pglogrepl.TypeMessage:
		intLogicalMsg := pgtypes.TypeMessage(*logicalMsg)
		rh.logger.Debugf("EVENT: %s", intLogicalMsg.String())
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnTypeEvent(xld, &intLogicalMsg)
				},
			)
		})
	case *pglogrepl.OriginMessage:
		intLogicalMsg := pgtypes.OriginMessage(*logicalMsg)
		rh.logger.Debugf("EVENT: %s", intLogicalMsg.String())
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifyLogicalReplicationEventHandler(
				func(handler eventhandlers.LogicalReplicationEventHandler) error {
					return handler.OnOriginEvent(xld, &intLogicalMsg)
				},
			)
		})
	case *pgtypes.LogicalReplicationMessage:
		rh.logger.Debugf("EVENT: %+v", logicalMsg.String())
		return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
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

func (rh *replicationHandler) handleDeleteMessage(xld pgtypes.XLogData, msg *pglogrepl.DeleteMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		rh.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode tuples
	oldValues, err := pgdecoding.DecodeTuples(rel, msg.OldTuple)
	if err != nil {
		return err
	}

	// Clean memory of the original tuple data
	msg.OldTuple = nil

	// Adapt the message object
	internalMsg := &pgtypes.DeleteMessage{
		DeleteMessage: msg,
		OldValues:     oldValues,
	}
	rh.logger.Debugf("EVENT: %+v", internalMsg)

	return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandlers.LogicalReplicationEventHandler) error {
				return handler.OnDeleteEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) handleUpdateMessage(xld pgtypes.XLogData, msg *pglogrepl.UpdateMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		rh.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode tuples
	oldValues, err := pgdecoding.DecodeTuples(rel, msg.OldTuple)
	if err != nil {
		return err
	}
	newValues, err := pgdecoding.DecodeTuples(rel, msg.NewTuple)
	if err != nil {
		return err
	}

	// Clean memory of the original tuple data
	msg.OldTuple = nil
	msg.NewTuple = nil

	// Adapt the message object
	internalMsg := &pgtypes.UpdateMessage{
		UpdateMessage: msg,
		OldValues:     oldValues,
		NewValues:     newValues,
	}
	rh.logger.Debugf("EVENT: %+v", internalMsg)

	return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandlers.LogicalReplicationEventHandler) error {
				return handler.OnUpdateEvent(xld, internalMsg)
			},
		)
	})
}

func (rh *replicationHandler) handleInsertMessage(xld pgtypes.XLogData, msg *pglogrepl.InsertMessage) error {
	rel, ok := rh.relations[msg.RelationID]
	if !ok {
		rh.logger.Fatalf("unknown relation ID %d", msg.RelationID)
	}

	// Decode tuples
	newValues, err := pgdecoding.DecodeTuples(rel, msg.Tuple)
	if err != nil {
		return err
	}

	// Clean memory of the original tuple data
	msg.Tuple = nil

	// Adapt the message object
	internalMsg := &pgtypes.InsertMessage{
		InsertMessage: msg,
		NewValues:     newValues,
	}
	rh.logger.Debugf("EVENT: %+v", internalMsg)

	return rh.taskManager.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifyLogicalReplicationEventHandler(
			func(handler eventhandlers.LogicalReplicationEventHandler) error {
				return handler.OnInsertEvent(xld, internalMsg)
			},
		)
	})
}
