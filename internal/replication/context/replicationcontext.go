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
	"context"
	"encoding"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	intschema "github.com/noctarius/timescaledb-event-streamer/internal/eventing/schema"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	intversion "github.com/noctarius/timescaledb-event-streamer/internal/version"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
	"github.com/urfave/cli"
	"sync"
)

const stateContextName = "snapshotContext"

type replicationContext struct {
	pgxConfig *pgx.ConnConfig

	sideChannel *sideChannelImpl
	dispatcher  *dispatcher

	// internal manager classes
	publicationManager *publicationManager
	stateManager       *stateManager
	schemaManager      *schemaManager
	taskManager        *taskManager

	snapshotInitialMode     spiconfig.InitialSnapshotMode
	snapshotBatchSize       int
	publicationName         string
	publicationCreate       bool
	publicationAutoDrop     bool
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

func NewReplicationContext(config *spiconfig.Config, pgxConfig *pgx.ConnConfig,
	namingStrategy namingstrategy.NamingStrategy, stateStorage statestorage.Storage) (ReplicationContext, error) {

	publicationName := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlPublicationName, "",
	)
	publicationCreate := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlPublicationCreate, true,
	)
	publicationAutoDrop := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlPublicationAutoDrop, true,
	)
	snapshotInitialMode := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlSnapshotInitialMode, spiconfig.Never,
	)
	snapshotBatchSize := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlSnapshotBatchsize, 1000,
	)
	replicationSlotName := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlReplicationSlotName, supporting.RandomTextString(20),
	)
	replicationSlotCreate := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlReplicationSlotCreate, true,
	)
	replicationSlotAutoDrop := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlReplicationSlotAutoDrop, true,
	)

	taskDispatcher, err := newDispatcher()
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// Build the replication context to be passed along in terms of
	// potential interface implementations to break up internal dependencies
	replicationContext := &replicationContext{
		pgxConfig: pgxConfig,

		dispatcher: taskDispatcher,

		snapshotInitialMode:     snapshotInitialMode,
		snapshotBatchSize:       snapshotBatchSize,
		publicationName:         publicationName,
		publicationCreate:       publicationCreate,
		publicationAutoDrop:     publicationAutoDrop,
		replicationSlotName:     replicationSlotName,
		replicationSlotCreate:   replicationSlotCreate,
		replicationSlotAutoDrop: replicationSlotAutoDrop,
	}

	// Instantiate the actual side channel implementation
	// which handles queries against the database
	sideChannel, err := newSideChannel(replicationContext)
	if err != nil {
		return nil, err
	}
	replicationContext.sideChannel = sideChannel

	pgVersion, err := sideChannel.getPostgresVersion()
	if err != nil {
		return nil, err
	}
	replicationContext.pgVersion = pgVersion

	tsdbVersion, found, err := sideChannel.getTimescaleDBVersion()
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cli.NewExitError("TimescaleDB extension not found", 17)
	}
	replicationContext.tsdbVersion = tsdbVersion

	databaseName, systemId, timeline, err := sideChannel.getSystemInformation()
	if err != nil {
		return nil, err
	}
	replicationContext.databaseName = databaseName
	replicationContext.systemId = systemId
	replicationContext.timeline = timeline

	walLevel, err := sideChannel.getWalLevel()
	if err != nil {
		return nil, err
	}
	replicationContext.walLevel = walLevel

	// Set up internal manager classes
	replicationContext.publicationManager = &publicationManager{
		replicationContext: replicationContext,
	}
	replicationContext.stateManager = &stateManager{
		stateStorage: stateStorage,
	}
	replicationContext.taskManager = &taskManager{
		taskDispatcher: taskDispatcher,
	}
	replicationContext.schemaManager = &schemaManager{
		namingStrategy: namingStrategy,
		topicPrefix:    config.Topic.Prefix,
	}
	// Instantiate the schema registry, keeping track of hypertable schemas
	// for the schema generation on event creation
	replicationContext.schemaManager.schemaRegistry = intschema.NewRegistry(replicationContext.schemaManager)

	return replicationContext, nil
}

func (rc *replicationContext) PublicationManager() PublicationManager {
	return rc.publicationManager
}

func (rc *replicationContext) StateManager() StateManager {
	return rc.stateManager
}

func (rc *replicationContext) SchemaManager() SchemaManager {
	return rc.schemaManager
}

func (rc *replicationContext) TaskManager() TaskManager {
	return rc.taskManager
}

func (rc *replicationContext) StartReplicationContext() error {
	rc.dispatcher.StartDispatcher()
	return rc.stateManager.start()
}

func (rc *replicationContext) StopReplicationContext() error {
	if err := rc.dispatcher.StopDispatcher(); err != nil {
		return err
	}
	return rc.stateManager.stop()
}

func (rc *replicationContext) Offset() (*statestorage.Offset, error) {
	offsets, err := rc.stateManager.get()
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

func (rc *replicationContext) SetLastTransactionId(xid uint32) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastTransactionId = xid
}

func (rc *replicationContext) LastTransactionId() uint32 {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastTransactionId
}

func (rc *replicationContext) SetLastBeginLSN(lsn pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastBeginLSN = lsn
}

func (rc *replicationContext) LastBeginLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastBeginLSN
}

func (rc *replicationContext) SetLastCommitLSN(lsn pgtypes.LSN) {
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

func (rc *replicationContext) AcknowledgeReceived(xld pgtypes.XLogData) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastReceivedLSN = pgtypes.LSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
}

func (rc *replicationContext) AcknowledgeProcessed(xld pgtypes.XLogData, processedLSN *pgtypes.LSN) error {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	newLastProcessedLSN := pgtypes.LSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
	if processedLSN != nil {
		rc.dispatcher.logger.Debugf("Acknowledge transaction end: %s", processedLSN.String())
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

	return rc.stateManager.set(rc.replicationSlotName, o)
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
	return rc.pgVersion >= intversion.PG_MIN_VERSION
}

func (rc *replicationContext) IsPG14GE() bool {
	return rc.pgVersion >= intversion.PG_14_VERSION
}

func (rc *replicationContext) IsMinimumTimescaleVersion() bool {
	return rc.tsdbVersion >= intversion.TSDB_MIN_VERSION
}

func (rc *replicationContext) IsTSDB212GE() bool {
	return rc.tsdbVersion >= intversion.TSDB_212_VERSION
}

func (rc *replicationContext) IsLogicalReplicationEnabled() bool {
	return rc.walLevel == "logical"
}

// ----> SideChannel functions

func (rc *replicationContext) HasTablePrivilege(
	entity systemcatalog.SystemEntity, grant Grant) (access bool, err error) {

	return rc.sideChannel.hasTablePrivilege(rc.pgxConfig.User, entity, grant)
}

func (rc *replicationContext) LoadHypertables(cb func(hypertable *systemcatalog.Hypertable) error) error {
	return rc.sideChannel.readHypertables(cb)
}

func (rc *replicationContext) LoadChunks(cb func(chunk *systemcatalog.Chunk) error) error {
	return rc.sideChannel.readChunks(cb)
}

func (rc *replicationContext) ReadHypertableSchema(
	cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool,
	hypertables ...*systemcatalog.Hypertable) error {

	return rc.sideChannel.readHypertableSchema(cb, hypertables...)
}

func (rc *replicationContext) SnapshotChunkTable(chunk *systemcatalog.Chunk,
	cb func(lsn pgtypes.LSN, values map[string]any) error) (pgtypes.LSN, error) {

	return rc.sideChannel.snapshotChunkTable(chunk, rc.snapshotBatchSize, cb)
}

func (rc *replicationContext) FetchHypertableSnapshotBatch(hypertable *systemcatalog.Hypertable, snapshotName string,
	cb func(lsn pgtypes.LSN, values map[string]any) error) error {

	return rc.sideChannel.fetchHypertableSnapshotBatch(hypertable, snapshotName, rc.snapshotBatchSize, cb)
}

func (rc *replicationContext) ReadSnapshotHighWatermark(
	hypertable *systemcatalog.Hypertable, snapshotName string) (map[string]any, error) {

	return rc.sideChannel.readSnapshotHighWatermark(hypertable, snapshotName)
}

func (rc *replicationContext) ReadReplicaIdentity(entity systemcatalog.SystemEntity) (pgtypes.ReplicaIdentity, error) {
	return rc.sideChannel.readReplicaIdentity(entity.SchemaName(), entity.TableName())
}

func (rc *replicationContext) ReadContinuousAggregate(
	materializedHypertableId int32) (viewSchema, viewName string, found bool, err error) {

	return rc.sideChannel.readContinuousAggregate(materializedHypertableId)
}

func (rc *replicationContext) NewReplicationConnection() (*ReplicationConnection, error) {
	return newReplicationConnection(rc)
}

func (rc *replicationContext) newReplicationChannelConnection(ctx context.Context) (*pgconn.PgConn, error) {
	connConfig := rc.pgxConfig.Config.Copy()
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}
	connConfig.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, connConfig)
}

func (rc *replicationContext) newSideChannelConnection(ctx context.Context) (*pgx.Conn, error) {
	return pgx.ConnectConfig(ctx, rc.pgxConfig)
}

func (rc *replicationContext) setPositionLSNs(receivedLSN, processedLSN pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastReceivedLSN = receivedLSN
	rc.lastProcessedLSN = processedLSN
}

func (rc *replicationContext) positionLSNs() (receivedLSN, processedLSN pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastReceivedLSN, rc.lastProcessedLSN
}

type publicationManager struct {
	replicationContext *replicationContext
}

func (pm *publicationManager) PublicationName() string {
	return pm.replicationContext.publicationName
}

func (pm *publicationManager) PublicationCreate() bool {
	return pm.replicationContext.publicationCreate
}

func (pm *publicationManager) PublicationAutoDrop() bool {
	return pm.replicationContext.publicationAutoDrop
}

func (pm *publicationManager) ExistsTableInPublication(entity systemcatalog.SystemEntity) (found bool, err error) {
	return pm.replicationContext.sideChannel.existsTableInPublication(
		pm.PublicationName(), entity.SchemaName(), entity.TableName(),
	)
}

func (pm *publicationManager) AttachTablesToPublication(entities ...systemcatalog.SystemEntity) error {
	return pm.replicationContext.sideChannel.attachTablesToPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) DetachTablesFromPublication(entities ...systemcatalog.SystemEntity) error {
	return pm.replicationContext.sideChannel.detachTablesFromPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) ReadPublishedTables() ([]systemcatalog.SystemEntity, error) {
	return pm.replicationContext.sideChannel.readPublishedTables(pm.PublicationName())
}

func (pm *publicationManager) CreatePublication() (bool, error) {
	return pm.replicationContext.sideChannel.createPublication(pm.PublicationName())
}

func (pm *publicationManager) ExistsPublication() (bool, error) {
	return pm.replicationContext.sideChannel.existsPublication(pm.PublicationName())
}

func (pm *publicationManager) DropPublication() error {
	return pm.replicationContext.sideChannel.dropPublication(pm.PublicationName())
}

type stateManager struct {
	stateStorage statestorage.Storage
}

func (sm *stateManager) start() error {
	return sm.stateStorage.Start()
}

func (sm *stateManager) stop() error {
	return sm.stateStorage.Stop()
}

func (sm *stateManager) get() (map[string]*statestorage.Offset, error) {
	return sm.stateStorage.Get()
}

func (sm *stateManager) set(key string, value *statestorage.Offset) error {
	return sm.stateStorage.Set(key, value)
}

func (sm *stateManager) StateEncoder(name string, encoder encoding.BinaryMarshaler) error {
	return sm.stateStorage.StateEncoder(name, encoder)
}

func (sm *stateManager) StateDecoder(name string, decoder encoding.BinaryUnmarshaler) (present bool, err error) {
	return sm.stateStorage.StateDecoder(name, decoder)
}

func (sm *stateManager) SetEncodedState(name string, encodedState []byte) {
	sm.stateStorage.SetEncodedState(name, encodedState)
}

func (sm *stateManager) EncodedState(name string) (encodedState []byte, present bool) {
	return sm.stateStorage.EncodedState(name)
}

func (sm *stateManager) SnapshotContext() (*watermark.SnapshotContext, error) {
	snapshotContext := &watermark.SnapshotContext{}
	present, err := sm.StateDecoder(stateContextName, snapshotContext)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	if !present {
		return nil, nil
	}
	return snapshotContext, nil
}

func (sm *stateManager) SnapshotContextTransaction(snapshotName string,
	createIfNotExists bool, transaction func(snapshotContext *watermark.SnapshotContext) error) error {

	retrieval := func() (*watermark.SnapshotContext, error) {
		return sm.SnapshotContext()
	}

	if createIfNotExists {
		retrieval = func() (*watermark.SnapshotContext, error) {
			return sm.getOrCreateSnapshotContext(snapshotName)
		}
	}

	snapshotContext, err := retrieval()
	if err != nil {
		return err
	}

	if snapshotContext == nil && !createIfNotExists {
		return errors.Errorf("No such snapshot context found")
	}

	if err := transaction(snapshotContext); err != nil {
		return err
	}

	if err := sm.setSnapshotContext(snapshotContext); err != nil {
		return err
	}
	return nil
}

func (sm *stateManager) setSnapshotContext(snapshotContext *watermark.SnapshotContext) error {
	return sm.StateEncoder(stateContextName, snapshotContext)
}

func (sm *stateManager) getOrCreateSnapshotContext(
	snapshotName string) (*watermark.SnapshotContext, error) {

	snapshotContext, err := sm.SnapshotContext()
	if err != nil {
		return nil, err
	}

	// Exists -> done
	if snapshotContext != nil {
		return snapshotContext, nil
	}

	// New snapshot context
	snapshotContext = watermark.NewSnapshotContext(snapshotName)

	// Register new snapshot context
	if err := sm.setSnapshotContext(snapshotContext); err != nil {
		return nil, err
	}

	return snapshotContext, nil
}

type schemaManager struct {
	schemaRegistry schema.Registry
	namingStrategy namingstrategy.NamingStrategy
	topicPrefix    string
}

func (sm *schemaManager) TopicPrefix() string {
	return sm.topicPrefix
}

func (sm *schemaManager) EventTopicName(hypertable *systemcatalog.Hypertable) string {
	return sm.namingStrategy.EventTopicName(sm.topicPrefix, hypertable)
}

func (sm *schemaManager) SchemaTopicName(hypertable *systemcatalog.Hypertable) string {
	return sm.namingStrategy.SchemaTopicName(sm.topicPrefix, hypertable)
}

func (sm *schemaManager) MessageTopicName() string {
	return sm.namingStrategy.MessageTopicName(sm.topicPrefix)
}

func (sm *schemaManager) RegisterSchema(schemaName string, schema schema.Struct) {
	sm.schemaRegistry.RegisterSchema(schemaName, schema)
}

func (sm *schemaManager) GetSchema(schemaName string) schema.Struct {
	return sm.schemaRegistry.GetSchema(schemaName)
}

func (sm *schemaManager) GetSchemaOrCreate(schemaName string, creator func() schema.Struct) schema.Struct {
	return sm.schemaRegistry.GetSchemaOrCreate(schemaName, creator)
}

func (sm *schemaManager) HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string {
	return sm.schemaRegistry.HypertableEnvelopeSchemaName(hypertable)
}

func (sm *schemaManager) HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string {
	return sm.schemaRegistry.HypertableKeySchemaName(hypertable)
}

func (sm *schemaManager) MessageEnvelopeSchemaName() string {
	return sm.schemaRegistry.MessageEnvelopeSchemaName()
}

type taskManager struct {
	taskDispatcher *dispatcher
}

func (tm *taskManager) RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
	tm.taskDispatcher.RegisterReplicationEventHandler(handler)
}

func (tm *taskManager) EnqueueTask(task Task) error {
	return tm.taskDispatcher.EnqueueTask(task)
}

func (tm *taskManager) RunTask(task Task) error {
	return tm.taskDispatcher.RunTask(task)
}

func (tm *taskManager) EnqueueTaskAndWait(task Task) error {
	return tm.taskDispatcher.EnqueueTaskAndWait(task)
}
