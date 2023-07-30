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
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func Test_ReplicationConnection_locateRestartLSN_empty(
	t *testing.T,
) {

	replicationContext := &testReplicationContext{
		replicationSlotName: "test",
		stateStorageManager: statestorage.NewStateStorageManager(statestorage.NewDummyStateStorage()),
		readReplicationSlot: func(slotName string) (
			pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
		) {

			return "pgoutput", "logical", 0, 0, nil
		},
	}

	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	identification := pglogrepl.IdentifySystemResult{
		SystemID: "123456789-987654321",
		Timeline: 1,
		XLogPos:  10000,
		DBName:   "test",
	}

	replicationConnection := &ReplicationConnection{
		logger:                 logger,
		identification:         identification,
		replicationContext:     replicationContext,
		replicationSlotCreated: false,
	}

	restartPoint, err := replicationConnection.locateRestartLSN()
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(10000))
}

func Test_ReplicationConnection_locateRestartLSN_from_offset(
	t *testing.T,
) {

	stateStorageManager := statestorage.NewStateStorageManager(statestorage.NewDummyStateStorage())
	replicationContext := &testReplicationContext{
		replicationSlotName: "test",
		stateStorageManager: stateStorageManager,
		readReplicationSlot: func(slotName string) (
			pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
		) {

			return "pgoutput", "logical", 0, 0, nil
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := stateStorageManager.Set(replicationContext.replicationSlotName, offset); err != nil {
		t.Errorf("error: %+v", err)
	}

	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	identification := pglogrepl.IdentifySystemResult{
		SystemID: "123456789-987654321",
		Timeline: 1,
		XLogPos:  10000,
		DBName:   "test",
	}

	replicationConnection := &ReplicationConnection{
		logger:                 logger,
		identification:         identification,
		replicationContext:     replicationContext,
		replicationSlotCreated: false,
	}

	restartPoint, err := replicationConnection.locateRestartLSN()
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(20000))
}

func Test_ReplicationConnection_locateRestartLSN_from_confirmed_flush_LSN_larger(
	t *testing.T,
) {

	stateStorageManager := statestorage.NewStateStorageManager(statestorage.NewDummyStateStorage())
	replicationContext := &testReplicationContext{
		replicationSlotName: "test",
		stateStorageManager: stateStorageManager,
		readReplicationSlot: func(slotName string) (
			pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
		) {

			return "pgoutput", "logical", 21000, 21000, nil
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := stateStorageManager.Set(replicationContext.replicationSlotName, offset); err != nil {
		t.Errorf("error: %+v", err)
	}

	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	identification := pglogrepl.IdentifySystemResult{
		SystemID: "123456789-987654321",
		Timeline: 1,
		XLogPos:  10000,
		DBName:   "test",
	}

	replicationConnection := &ReplicationConnection{
		logger:                 logger,
		identification:         identification,
		replicationContext:     replicationContext,
		replicationSlotCreated: false,
	}

	restartPoint, err := replicationConnection.locateRestartLSN()
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(21000))
}

func Test_ReplicationConnection_locateRestartLSN_from_confirmed_flush_LSN_smaller(
	t *testing.T,
) {

	stateStorageManager := statestorage.NewStateStorageManager(statestorage.NewDummyStateStorage())
	replicationContext := &testReplicationContext{
		replicationSlotName: "test",
		stateStorageManager: stateStorageManager,
		readReplicationSlot: func(slotName string) (
			pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
		) {

			return "pgoutput", "logical", 19000, 19000, nil
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := stateStorageManager.Set(replicationContext.replicationSlotName, offset); err != nil {
		t.Errorf("error: %+v", err)
	}

	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	identification := pglogrepl.IdentifySystemResult{
		SystemID: "123456789-987654321",
		Timeline: 1,
		XLogPos:  10000,
		DBName:   "test",
	}

	replicationConnection := &ReplicationConnection{
		logger:                 logger,
		identification:         identification,
		replicationContext:     replicationContext,
		replicationSlotCreated: false,
	}

	restartPoint, err := replicationConnection.locateRestartLSN()
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(20000))
}

func Test_ReplicationConnection_locateRestartLSN_error_physical_slot(
	t *testing.T,
) {

	stateStorageManager := statestorage.NewStateStorageManager(statestorage.NewDummyStateStorage())
	replicationContext := &testReplicationContext{
		replicationSlotName: "test",
		stateStorageManager: stateStorageManager,
		readReplicationSlot: func(slotName string) (
			pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
		) {

			return "pgoutput", "physical", 19000, 19000, nil
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := stateStorageManager.Set(replicationContext.replicationSlotName, offset); err != nil {
		t.Errorf("error: %+v", err)
	}

	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	identification := pglogrepl.IdentifySystemResult{
		SystemID: "123456789-987654321",
		Timeline: 1,
		XLogPos:  10000,
		DBName:   "test",
	}

	replicationConnection := &ReplicationConnection{
		logger:                 logger,
		identification:         identification,
		replicationContext:     replicationContext,
		replicationSlotCreated: false,
	}

	_, err = replicationConnection.locateRestartLSN()
	assert.Error(t, err, "illegal slot type found for existing replication slot 'test', expected logical but found physical")
}

func Test_ReplicationConnection_locateRestartLSN_error_plugin_name(
	t *testing.T,
) {

	stateStorageManager := statestorage.NewStateStorageManager(statestorage.NewDummyStateStorage())
	replicationContext := &testReplicationContext{
		replicationSlotName: "test",
		stateStorageManager: stateStorageManager,
		readReplicationSlot: func(slotName string) (
			pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
		) {

			return "json", "logical", 19000, 19000, nil
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := stateStorageManager.Set(replicationContext.replicationSlotName, offset); err != nil {
		t.Errorf("error: %+v", err)
	}

	logger, err := logging.NewLogger("ReplicationConnection")
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	identification := pglogrepl.IdentifySystemResult{
		SystemID: "123456789-987654321",
		Timeline: 1,
		XLogPos:  10000,
		DBName:   "test",
	}

	replicationConnection := &ReplicationConnection{
		logger:                 logger,
		identification:         identification,
		replicationContext:     replicationContext,
		replicationSlotCreated: false,
	}

	_, err = replicationConnection.locateRestartLSN()
	assert.Error(t, err, "illegal plugin name found for existing replication slot 'test', expected pgoutput but found json")
}

type testReplicationContext struct {
	stateStorageManager statestorage.Manager
	replicationSlotName string
	readReplicationSlot func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	)
}

func (t testReplicationContext) StartReplicationContext() error {
	return nil
}

func (t testReplicationContext) StopReplicationContext() error {
	return nil
}

func (t testReplicationContext) NewReplicationChannelConnection(
	ctx context.Context,
) (*pgconn.PgConn, error) {

	return nil, nil
}

func (t testReplicationContext) Offset() (*statestorage.Offset, error) {
	offsets, err := t.stateStorageManager.Get()
	if err != nil {
		return nil, err
	}
	if offsets == nil {
		return nil, nil
	}
	if o, present := offsets[t.replicationSlotName]; present {
		return o, nil
	}
	return nil, nil
}

func (t testReplicationContext) SetLastTransactionId(
	xid uint32,
) {
}

func (t testReplicationContext) LastTransactionId() uint32 {
	return 0
}

func (t testReplicationContext) SetLastBeginLSN(
	lsn pgtypes.LSN,
) {
}

func (t testReplicationContext) LastBeginLSN() pgtypes.LSN {
	return 0
}

func (t testReplicationContext) SetLastCommitLSN(
	lsn pgtypes.LSN,
) {
}

func (t testReplicationContext) LastCommitLSN() pgtypes.LSN {
	return 0
}

func (t testReplicationContext) AcknowledgeReceived(
	xld pgtypes.XLogData,
) {
}

func (t testReplicationContext) LastReceivedLSN() pgtypes.LSN {
	return 0
}

func (t testReplicationContext) AcknowledgeProcessed(
	xld pgtypes.XLogData, processedLSN *pgtypes.LSN,
) error {

	return nil
}

func (t testReplicationContext) LastProcessedLSN() pgtypes.LSN {
	return 0
}

func (t testReplicationContext) SetPositionLSNs(
	receivedLSN, processedLSN pgtypes.LSN,
) {
}

func (t testReplicationContext) InitialSnapshotMode() config.InitialSnapshotMode {
	return ""
}

func (t testReplicationContext) DatabaseUsername() string {
	return ""
}

func (t testReplicationContext) ReplicationSlotName() string {
	return t.replicationSlotName
}

func (t testReplicationContext) ReplicationSlotCreate() bool {
	return false
}

func (t testReplicationContext) ReplicationSlotAutoDrop() bool {
	return false
}

func (t testReplicationContext) WALLevel() string {
	return ""
}

func (t testReplicationContext) SystemId() string {
	return ""
}

func (t testReplicationContext) Timeline() int32 {
	return 0
}

func (t testReplicationContext) DatabaseName() string {
	return ""
}

func (t testReplicationContext) PostgresVersion() version.PostgresVersion {
	return 0
}

func (t testReplicationContext) TimescaleVersion() version.TimescaleVersion {
	return 0
}

func (t testReplicationContext) IsMinimumPostgresVersion() bool {
	return false
}

func (t testReplicationContext) IsPG14GE() bool {
	return false
}

func (t testReplicationContext) IsMinimumTimescaleVersion() bool {
	return false
}

func (t testReplicationContext) IsTSDB212GE() bool {
	return false
}

func (t testReplicationContext) IsLogicalReplicationEnabled() bool {
	return false
}

func (t testReplicationContext) ExistsReplicationSlot(
	slotName string,
) (found bool, err error) {

	return false, nil
}

func (t testReplicationContext) ReadReplicationSlot(
	slotName string,
) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error) {

	return t.readReplicationSlot(slotName)
}
