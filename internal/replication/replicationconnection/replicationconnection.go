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

package replicationconnection

import (
	stdcontext "context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"time"
)

const outputPlugin = "pgoutput"

type ReplicationConnection struct {
	logger             *logging.Logger
	replicationContext context.ReplicationContext

	conn                   *pgconn.PgConn
	identification         pglogrepl.IdentifySystemResult
	replicationSlotCreated bool
}

func NewReplicationConnection(replicationContext context.ReplicationContext) (*ReplicationConnection, error) {
	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		return nil, err
	}

	rc := &ReplicationConnection{
		logger:             logger,
		replicationContext: replicationContext,
	}

	if err := rc.reconnect(); err != nil {
		return nil, err
	}

	identification, err := rc.identifySystem()
	if err != nil {
		return nil, err
	}
	rc.identification = identification

	rc.logger.Infof("SystemId: %s, Timeline: %d, XLogPos: %s, DatabaseName: %s",
		identification.SystemID, identification.Timeline, identification.XLogPos, identification.DBName,
	)
	return rc, nil
}

func (rc *ReplicationConnection) ReceiveMessage(deadline time.Time) (pgproto3.BackendMessage, error) {
	ctx, cancel := stdcontext.WithDeadline(stdcontext.Background(), deadline)
	defer cancel()

	msg, err := rc.conn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("ReceiveMessage failed: %s", err)
	}
	return msg, nil
}

func (rc *ReplicationConnection) SendStatusUpdate() error {
	processedLSN := rc.replicationContext.LastProcessedLSN()
	if err := pglogrepl.SendStandbyStatusUpdate(stdcontext.Background(), rc.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pglogrepl.LSN(processedLSN) + 1,
			WALApplyPosition: pglogrepl.LSN(processedLSN) + 1,
		},
	); err != nil {
		rc.logger.Fatalln("SendStandbyStatusUpdate failed:", err)
	}
	return nil
}

func (rc *ReplicationConnection) StartReplication(pluginArguments []string) (pgtypes.LSN, error) {
	restartLSN, err := rc.locateRestartLSN()
	if err != nil {
		return 0, errors.Wrap(err, 0)
	}

	// Configure initial LSN in case there isn't anything immediate to handle
	// we don't want to send LSN 0 to the server
	rc.replicationContext.SetPositionLSNs(restartLSN, restartLSN)

	if err := pglogrepl.StartReplication(stdcontext.Background(), rc.conn,
		rc.replicationContext.ReplicationSlotName(), pglogrepl.LSN(restartLSN),
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		},
	); err != nil {
		if err := rc.reconnect(); err != nil {
			return 0, errors.Wrap(err, 0)
		}

		return restartLSN, pglogrepl.StartReplication(stdcontext.Background(), rc.conn,
			rc.replicationContext.ReplicationSlotName(), pglogrepl.LSN(restartLSN),
			pglogrepl.StartReplicationOptions{
				PluginArgs: pluginArguments,
			},
		)
	}
	return restartLSN, nil
}

func (rc *ReplicationConnection) StopReplication() error {
	_, err := pglogrepl.SendStandbyCopyDone(stdcontext.Background(), rc.conn)
	if e, ok := err.(*pgconn.PgError); ok {
		if e.Code == pgerrcode.InternalError {
			return nil
		}
	}
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (rc *ReplicationConnection) CreateReplicationSlot() (slotName, snapshotName string, created bool, err error) {
	if !rc.replicationContext.ReplicationSlotCreate() {
		return "", "", false, nil
	}

	replicationSlotName := rc.replicationContext.ReplicationSlotName()
	found, err := rc.replicationContext.ExistsReplicationSlot(replicationSlotName)
	if err != nil {
		return "", "", false, errors.Wrap(err, 0)
	}

	if found {
		return replicationSlotName, "", false, nil
	}

	slot, err := pglogrepl.CreateReplicationSlot(stdcontext.Background(), rc.conn, replicationSlotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			SnapshotAction: "EXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		return "", "", false, errors.Wrap(err, 0)
	}

	rc.replicationSlotCreated = true
	return slot.SlotName, slot.SnapshotName, true, nil
}

func (rc *ReplicationConnection) DropReplicationSlot() error {
	if !rc.replicationSlotCreated || !rc.replicationContext.ReplicationSlotAutoDrop() {
		return nil
	}
	if err := pglogrepl.DropReplicationSlot(stdcontext.Background(), rc.conn, rc.replicationContext.ReplicationSlotName(),
		pglogrepl.DropReplicationSlotOptions{
			Wait: true,
		},
	); err != nil {
		return errors.Wrap(err, 0)
	}
	rc.logger.Infoln("Dropped replication slot")
	return nil
}

func (rc *ReplicationConnection) Close() error {
	return rc.conn.Close(stdcontext.Background())
}

func (rc *ReplicationConnection) reconnect() error {
	conn, err := rc.replicationContext.NewReplicationChannelConnection(stdcontext.Background())
	if err != nil {
		return errors.Wrap(err, 0)
	}
	rc.conn = conn
	return nil
}

func (rc *ReplicationConnection) identifySystem() (pglogrepl.IdentifySystemResult, error) {
	return pglogrepl.IdentifySystem(stdcontext.Background(), rc.conn)
}

func (rc *ReplicationConnection) locateRestartLSN() (pgtypes.LSN, error) {
	replicationSlotName := rc.replicationContext.ReplicationSlotName()

	offset, err := rc.replicationContext.Offset()
	if err != nil {
		return 0, errors.Wrap(err, 0)
	}

	pluginName, slotType, _, confirmedFlushLSN, err := rc.replicationContext.ReadReplicationSlot(replicationSlotName)
	if err != nil {
		return 0, errors.Wrap(err, 0)
	}

	restartLSN := confirmedFlushLSN
	if confirmedFlushLSN > 0 {
		if pluginName != "pgoutput" {
			return 0, errors.Errorf(
				"illegal plugin name found for existing replication slot '%s', expected pgoutput but found %s",
				replicationSlotName, pluginName,
			)
		}

		if slotType != "logical" {
			return 0, errors.Errorf(
				"illegal slot type found for existing replication slot '%s', expected logical but found %s",
				replicationSlotName, slotType,
			)
		}
	}

	if offset != nil && offset.LSN > restartLSN {
		restartLSN = offset.LSN
	}

	if restartLSN == 0 {
		restartLSN = pgtypes.LSN(rc.identification.XLogPos)
	}

	if restartLSN == confirmedFlushLSN && !rc.replicationSlotCreated {
		addMsg := ""
		if offset != nil {
			addMsg = fmt.Sprintf(" (lower offset LSN: %s)", offset.LSN)
		}
		rc.logger.Infof("Restarting replication at last confirmed flush LSN: %s%s", restartLSN, addMsg)
	} else if offset != nil && restartLSN == offset.LSN {
		rc.logger.Infof("Restarting replication at last LSN in offset storage: %s", restartLSN)
	} else {
		rc.logger.Infof("Starting replication at current LSN: %s", restartLSN)
	}
	return restartLSN, nil
}
