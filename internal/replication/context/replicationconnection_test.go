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

package context

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_ReplicationConnection_locateRestartLSN_empty(t *testing.T) {
	replicationContext := &replicationContext{
		replicationSlotName: "test",
		stateManager: &stateManager{
			stateStorage: statestorage.NewDummyStateStorage(),
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

	readReplicationSlot := func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	) {

		return "pgoutput", "logical", 0, 0, nil
	}

	restartPoint, err := replicationConnection.locateRestartLSN(readReplicationSlot)
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(10000))
}

func Test_ReplicationConnection_locateRestartLSN_from_offset(t *testing.T) {
	replicationContext := &replicationContext{
		replicationSlotName: "test",
		stateManager: &stateManager{
			stateStorage: statestorage.NewDummyStateStorage(),
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := replicationContext.stateManager.set(replicationContext.replicationSlotName, offset); err != nil {
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

	readReplicationSlot := func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	) {

		return "pgoutput", "logical", 0, 0, nil
	}

	restartPoint, err := replicationConnection.locateRestartLSN(readReplicationSlot)
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(20000))
}

func Test_ReplicationConnection_locateRestartLSN_from_confirmed_flush_LSN_larger(t *testing.T) {
	replicationContext := &replicationContext{
		replicationSlotName: "test",
		stateManager: &stateManager{
			stateStorage: statestorage.NewDummyStateStorage(),
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := replicationContext.stateManager.set(replicationContext.replicationSlotName, offset); err != nil {
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

	readReplicationSlot := func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	) {

		return "pgoutput", "logical", 21000, 21000, nil
	}

	restartPoint, err := replicationConnection.locateRestartLSN(readReplicationSlot)
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(21000))
}

func Test_ReplicationConnection_locateRestartLSN_from_confirmed_flush_LSN_smaller(t *testing.T) {
	replicationContext := &replicationContext{
		replicationSlotName: "test",
		stateManager: &stateManager{
			stateStorage: statestorage.NewDummyStateStorage(),
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := replicationContext.stateManager.set(replicationContext.replicationSlotName, offset); err != nil {
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

	readReplicationSlot := func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	) {

		return "pgoutput", "logical", 19000, 19000, nil
	}

	restartPoint, err := replicationConnection.locateRestartLSN(readReplicationSlot)
	if err != nil {
		t.Errorf("error: %+v", err)
	}

	assert.Equal(t, restartPoint, pgtypes.LSN(20000))
}

func Test_ReplicationConnection_locateRestartLSN_error_physical_slot(t *testing.T) {
	replicationContext := &replicationContext{
		replicationSlotName: "test",
		stateManager: &stateManager{
			stateStorage: statestorage.NewDummyStateStorage(),
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := replicationContext.stateManager.set(replicationContext.replicationSlotName, offset); err != nil {
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

	readReplicationSlot := func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	) {

		return "pgoutput", "physical", 19000, 19000, nil
	}

	_, err = replicationConnection.locateRestartLSN(readReplicationSlot)
	assert.Error(t, err, "illegal slot type found for existing replication slot 'test', expected logical but found physical")
}

func Test_ReplicationConnection_locateRestartLSN_error_plugin_name(t *testing.T) {
	replicationContext := &replicationContext{
		replicationSlotName: "test",
		stateManager: &stateManager{
			stateStorage: statestorage.NewDummyStateStorage(),
		},
	}

	offset := &statestorage.Offset{}
	offset.LSN = 20000
	offset.Timestamp = time.Now()

	if err := replicationContext.stateManager.set(replicationContext.replicationSlotName, offset); err != nil {
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

	readReplicationSlot := func(slotName string) (
		pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error,
	) {

		return "json", "logical", 19000, 19000, nil
	}

	_, err = replicationConnection.locateRestartLSN(readReplicationSlot)
	assert.Error(t, err, "illegal plugin name found for existing replication slot 'test', expected pgoutput but found json")
}
