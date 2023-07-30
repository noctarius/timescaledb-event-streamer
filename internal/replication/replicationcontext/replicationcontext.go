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

package replicationcontext

import (
	"context"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/samber/lo"
	"github.com/urfave/cli"
	"sync"
)

type replicationContext struct {
	pgxConfig *pgx.ConnConfig
	logger    *logging.Logger

	sideChannel         sidechannel.SideChannel
	stateStorageManager statestorage.Manager

	snapshotInitialMode     spiconfig.InitialSnapshotMode
	snapshotBatchSize       int
	replicationSlotName     string
	replicationSlotCreate   bool
	replicationSlotAutoDrop bool

	timeline          int32
	systemId          string
	databaseName      string
	walLevel          string
	lsnMutex          sync.Mutex
	lastBeginLSN      pgtypes.LSN
	lastCommitLSN     pgtypes.LSN
	lastReceivedLSN   pgtypes.LSN
	lastProcessedLSN  pgtypes.LSN
	lastTransactionId uint32

	pgVersion   version.PostgresVersion
	tsdbVersion version.TimescaleVersion
}

func NewReplicationContext(
	config *spiconfig.Config, pgxConfig *pgx.ConnConfig, stateStorageManager statestorage.Manager,
	sideChannel sidechannel.SideChannel,
) (replicationcontext.ReplicationContext, error) {

	snapshotInitialMode := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlSnapshotInitialMode, spiconfig.Never,
	)
	snapshotBatchSize := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlSnapshotBatchsize, 1000,
	)
	replicationSlotName := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlReplicationSlotName, lo.RandomString(20, lo.LowerCaseLettersCharset),
	)
	replicationSlotCreate := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlReplicationSlotCreate, true,
	)
	replicationSlotAutoDrop := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlReplicationSlotAutoDrop, true,
	)

	logger, err := logging.NewLogger("ReplicationContext")
	if err != nil {
		return nil, err
	}

	// Build the replication context to be passed along in terms of
	// potential interface implementations to break up internal dependencies
	replicationContext := &replicationContext{
		pgxConfig: pgxConfig,
		logger:    logger,

		sideChannel:         sideChannel,
		stateStorageManager: stateStorageManager,

		snapshotInitialMode:     snapshotInitialMode,
		snapshotBatchSize:       snapshotBatchSize,
		replicationSlotName:     replicationSlotName,
		replicationSlotCreate:   replicationSlotCreate,
		replicationSlotAutoDrop: replicationSlotAutoDrop,
	}

	pgVersion, err := sideChannel.GetPostgresVersion()
	if err != nil {
		return nil, err
	}
	replicationContext.pgVersion = pgVersion

	tsdbVersion, found, err := sideChannel.GetTimescaleDBVersion()
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cli.NewExitError("TimescaleDB extension not found", 17)
	}
	replicationContext.tsdbVersion = tsdbVersion

	databaseName, systemId, timeline, err := sideChannel.GetSystemInformation()
	if err != nil {
		return nil, err
	}
	replicationContext.databaseName = databaseName
	replicationContext.systemId = systemId
	replicationContext.timeline = timeline

	walLevel, err := sideChannel.GetWalLevel()
	if err != nil {
		return nil, err
	}
	replicationContext.walLevel = walLevel

	return replicationContext, nil
}

func (rc *replicationContext) StateStorageManager() statestorage.Manager {
	return rc.stateStorageManager
}

func (rc *replicationContext) StartReplicationContext() error {
	return rc.stateStorageManager.Start()
}

func (rc *replicationContext) StopReplicationContext() error {
	return rc.stateStorageManager.Stop()
}

func (rc *replicationContext) Offset() (*statestorage.Offset, error) {
	offsets, err := rc.stateStorageManager.Get()
	if err != nil {
		return nil, err
	}
	if offsets == nil {
		return nil, nil
	}
	if o, present := offsets[rc.replicationSlotName]; present {
		return o, nil
	}
	return nil, nil
}

func (rc *replicationContext) SetLastTransactionId(
	xid uint32,
) {

	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastTransactionId = xid
}

func (rc *replicationContext) LastTransactionId() uint32 {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastTransactionId
}

func (rc *replicationContext) SetLastBeginLSN(
	lsn pgtypes.LSN,
) {

	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastBeginLSN = lsn
}

func (rc *replicationContext) LastBeginLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastBeginLSN
}

func (rc *replicationContext) SetLastCommitLSN(
	lsn pgtypes.LSN,
) {

	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastCommitLSN = lsn
}

func (rc *replicationContext) LastCommitLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastCommitLSN
}

func (rc *replicationContext) LastReceivedLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastReceivedLSN
}

func (rc *replicationContext) LastProcessedLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastProcessedLSN
}

func (rc *replicationContext) SetPositionLSNs(
	receivedLSN, processedLSN pgtypes.LSN,
) {

	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastReceivedLSN = receivedLSN
	rc.lastProcessedLSN = processedLSN
}

func (rc *replicationContext) AcknowledgeReceived(
	xld pgtypes.XLogData,
) {

	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastReceivedLSN = pgtypes.LSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
}

func (rc *replicationContext) AcknowledgeProcessed(
	xld pgtypes.XLogData, processedLSN *pgtypes.LSN,
) error {

	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	newLastProcessedLSN := pgtypes.LSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
	if processedLSN != nil {
		rc.logger.Debugf("Acknowledge transaction end: %s", processedLSN)
		newLastProcessedLSN = *processedLSN
	}

	if newLastProcessedLSN > rc.lastProcessedLSN {
		rc.lastProcessedLSN = newLastProcessedLSN
	}

	o, err := rc.Offset()
	if err != nil {
		return err
	}

	if o == nil {
		o = &statestorage.Offset{}
	}

	o.LSN = rc.lastProcessedLSN
	o.Timestamp = xld.ServerTime

	return rc.stateStorageManager.Set(rc.replicationSlotName, o)
}

func (rc *replicationContext) InitialSnapshotMode() spiconfig.InitialSnapshotMode {
	return rc.snapshotInitialMode
}

func (rc *replicationContext) DatabaseUsername() string {
	return rc.pgxConfig.User
}

func (rc *replicationContext) ReplicationSlotName() string {
	return rc.replicationSlotName
}

func (rc *replicationContext) ReplicationSlotCreate() bool {
	return rc.replicationSlotCreate
}

func (rc *replicationContext) ReplicationSlotAutoDrop() bool {
	return rc.replicationSlotAutoDrop
}

func (rc *replicationContext) WALLevel() string {
	return rc.walLevel
}

func (rc *replicationContext) SystemId() string {
	return rc.systemId
}

func (rc *replicationContext) Timeline() int32 {
	return rc.timeline
}

func (rc *replicationContext) DatabaseName() string {
	return rc.databaseName
}

func (rc *replicationContext) PostgresVersion() version.PostgresVersion {
	return rc.pgVersion
}

func (rc *replicationContext) TimescaleVersion() version.TimescaleVersion {
	return rc.tsdbVersion
}

func (rc *replicationContext) IsMinimumPostgresVersion() bool {
	return rc.pgVersion >= version.PG_MIN_VERSION
}

func (rc *replicationContext) IsPG14GE() bool {
	return rc.pgVersion >= version.PG_14_VERSION
}

func (rc *replicationContext) IsMinimumTimescaleVersion() bool {
	return rc.tsdbVersion >= version.TSDB_MIN_VERSION
}

func (rc *replicationContext) IsTSDB212GE() bool {
	return rc.tsdbVersion >= version.TSDB_212_VERSION
}

func (rc *replicationContext) IsLogicalReplicationEnabled() bool {
	return rc.walLevel == "logical"
}

// ----> SideChannel functions

func (rc *replicationContext) HasTablePrivilege(
	entity systemcatalog.SystemEntity, grant sidechannel.TableGrant,
) (access bool, err error) {

	return rc.sideChannel.HasTablePrivilege(rc.pgxConfig.User, entity, grant)
}

func (rc *replicationContext) LoadHypertables(
	cb func(hypertable *systemcatalog.Hypertable) error,
) error {

	return rc.sideChannel.ReadHypertables(cb)
}

func (rc *replicationContext) LoadChunks(
	cb func(chunk *systemcatalog.Chunk) error,
) error {

	return rc.sideChannel.ReadChunks(cb)
}

func (rc *replicationContext) ReadHypertableSchema(
	cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool,
	pgTypeResolver func(oid uint32) (pgtypes.PgType, error), hypertables ...*systemcatalog.Hypertable,
) error {

	return rc.sideChannel.ReadHypertableSchema(cb, pgTypeResolver, hypertables...)
}

func (rc *replicationContext) SnapshotChunkTable(
	rowDecoderFactory pgtypes.RowDecoderFactory, chunk *systemcatalog.Chunk, cb sidechannel.SnapshotRowCallback,
) (pgtypes.LSN, error) {

	// FIXME: remove the intermediate function?
	return rc.sideChannel.SnapshotChunkTable(rowDecoderFactory, chunk, rc.snapshotBatchSize, cb)
}

func (rc *replicationContext) FetchHypertableSnapshotBatch(
	rowDecoderFactory pgtypes.RowDecoderFactory, hypertable *systemcatalog.Hypertable,
	snapshotName string, cb sidechannel.SnapshotRowCallback,
) error {

	// FIXME: remove the intermediate function?
	return rc.sideChannel.FetchHypertableSnapshotBatch(
		rowDecoderFactory, hypertable, snapshotName, rc.snapshotBatchSize, cb,
	)
}

func (rc *replicationContext) ReadSnapshotHighWatermark(
	rowDecoderFactory pgtypes.RowDecoderFactory, hypertable *systemcatalog.Hypertable, snapshotName string,
) (map[string]any, error) {

	// FIXME: remove the intermediate function?
	return rc.sideChannel.ReadSnapshotHighWatermark(rowDecoderFactory, hypertable, snapshotName)
}

func (rc *replicationContext) ReadReplicaIdentity(
	entity systemcatalog.SystemEntity,
) (pgtypes.ReplicaIdentity, error) {

	return rc.sideChannel.ReadReplicaIdentity(entity.SchemaName(), entity.TableName())
}

func (rc *replicationContext) ReadContinuousAggregate(
	materializedHypertableId int32,
) (viewSchema, viewName string, found bool, err error) {

	return rc.sideChannel.ReadContinuousAggregate(materializedHypertableId)
}

func (rc *replicationContext) ExistsReplicationSlot(
	slotName string,
) (found bool, err error) {

	return rc.sideChannel.ExistsReplicationSlot(slotName)
}

func (rc *replicationContext) ReadReplicationSlot(
	slotName string,
) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error) {

	return rc.sideChannel.ReadReplicationSlot(slotName)
}

func (rc *replicationContext) NewReplicationChannelConnection(
	ctx context.Context,
) (*pgconn.PgConn, error) {

	connConfig := rc.pgxConfig.Config.Copy()
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}
	connConfig.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, connConfig)
}
