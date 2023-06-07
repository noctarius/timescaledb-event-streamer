package context

import (
	"context"
	"encoding"
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
	"github.com/urfave/cli"
	"sync"
)

type ReplicationContext struct {
	pgxConfig *pgx.ConnConfig

	sideChannel    *sideChannelImpl
	dispatcher     *dispatcher
	schemaRegistry schema.Registry
	namingStrategy namingstrategy.NamingStrategy
	stateStorage   statestorage.Storage

	snapshotInitialMode     spiconfig.InitialSnapshotMode
	snapshotBatchSize       int
	publicationName         string
	publicationCreate       bool
	publicationAutoDrop     bool
	topicPrefix             string
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
	namingStrategy namingstrategy.NamingStrategy, stateStorage statestorage.Storage) (*ReplicationContext, error) {

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

	// Build the replication context to be passed along in terms of
	// potential interface implementations to break up internal dependencies
	replicationContext := &ReplicationContext{
		pgxConfig: pgxConfig,

		namingStrategy: namingStrategy,
		dispatcher:     newDispatcher(),
		stateStorage:   stateStorage,

		snapshotInitialMode:     snapshotInitialMode,
		snapshotBatchSize:       snapshotBatchSize,
		publicationName:         publicationName,
		publicationCreate:       publicationCreate,
		publicationAutoDrop:     publicationAutoDrop,
		topicPrefix:             config.Topic.Prefix,
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

	// Instantiate the schema registry, keeping track of hypertable schemas
	// for the schema generation on event creation
	replicationContext.schemaRegistry = intschema.NewRegistry(replicationContext)

	return replicationContext, nil
}

func (rc *ReplicationContext) StartReplicationContext() error {
	rc.dispatcher.StartDispatcher()
	return rc.stateStorage.Start()
}

func (rc *ReplicationContext) StopReplicationContext() error {
	if err := rc.dispatcher.StopDispatcher(); err != nil {
		return err
	}
	return rc.stateStorage.Stop()
}

func (rc *ReplicationContext) Offset() (*statestorage.Offset, error) {
	offsets, err := rc.stateStorage.Get()
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

func (rc *ReplicationContext) SetLastTransactionId(xid uint32) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastTransactionId = xid
}

func (rc *ReplicationContext) LastTransactionId() uint32 {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastTransactionId
}

func (rc *ReplicationContext) SetLastBeginLSN(lsn pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastBeginLSN = lsn
}

func (rc *ReplicationContext) LastBeginLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastBeginLSN
}

func (rc *ReplicationContext) SetLastCommitLSN(lsn pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastCommitLSN = lsn
}

func (rc *ReplicationContext) LastCommitLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastCommitLSN
}

func (rc *ReplicationContext) LastReceivedLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastReceivedLSN
}

func (rc *ReplicationContext) LastProcessedLSN() pgtypes.LSN {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastProcessedLSN
}

func (rc *ReplicationContext) AcknowledgeReceived(xld pgtypes.XLogData) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastReceivedLSN = pgtypes.LSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
}

func (rc *ReplicationContext) AcknowledgeProcessed(xld pgtypes.XLogData) error {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastProcessedLSN = pgtypes.LSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))

	o, err := rc.Offset()
	if err != nil {
		return err
	}

	if o == nil {
		o = &statestorage.Offset{
			LSN:       rc.lastProcessedLSN,
			Timestamp: xld.ServerTime,
		}
	}
	return rc.stateStorage.Set(rc.replicationSlotName, o)
}

func (rc *ReplicationContext) StateEncoder(name string, encoder encoding.BinaryMarshaler) error {
	return rc.stateStorage.StateEncoder(name, encoder)
}

func (rc *ReplicationContext) StateDecoder(name string, decoder encoding.BinaryUnmarshaler) (present bool, err error) {
	return rc.stateStorage.StateDecoder(name, decoder)
}

func (rc *ReplicationContext) SetEncodedState(name string, encodedState []byte) {
	rc.stateStorage.SetEncodedState(name, encodedState)
}

func (rc *ReplicationContext) EncodedState(name string) (encodedState []byte, present bool) {
	return rc.stateStorage.EncodedState(name)
}

func (rc *ReplicationContext) InitialSnapshotMode() spiconfig.InitialSnapshotMode {
	return rc.snapshotInitialMode
}

func (rc *ReplicationContext) DatabaseUsername() string {
	return rc.pgxConfig.User
}

func (rc *ReplicationContext) PublicationName() string {
	return rc.publicationName
}

func (rc *ReplicationContext) PublicationCreate() bool {
	return rc.publicationCreate
}

func (rc *ReplicationContext) PublicationAutoDrop() bool {
	return rc.publicationAutoDrop
}

func (rc *ReplicationContext) ReplicationSlotName() string {
	return rc.replicationSlotName
}

func (rc *ReplicationContext) ReplicationSlotCreate() bool {
	return rc.replicationSlotCreate
}

func (rc *ReplicationContext) ReplicationSlotAutoDrop() bool {
	return rc.replicationSlotAutoDrop
}

func (rc *ReplicationContext) WALLevel() string {
	return rc.walLevel
}

func (rc *ReplicationContext) SystemId() string {
	return rc.systemId
}

func (rc *ReplicationContext) Timeline() int32 {
	return rc.timeline
}

func (rc *ReplicationContext) DatabaseName() string {
	return rc.databaseName
}

func (rc *ReplicationContext) TopicPrefix() string {
	return rc.topicPrefix
}

func (rc *ReplicationContext) PostgresVersion() version.PostgresVersion {
	return rc.pgVersion
}

func (rc *ReplicationContext) TimescaleVersion() version.TimescaleVersion {
	return rc.tsdbVersion
}

func (rc *ReplicationContext) IsMinimumPostgresVersion() bool {
	return rc.pgVersion >= intversion.PG_MIN_VERSION
}

func (rc *ReplicationContext) IsPG14GE() bool {
	return rc.pgVersion >= intversion.PG_14_VERSION
}

func (rc *ReplicationContext) IsMinimumTimescaleVersion() bool {
	return rc.tsdbVersion >= intversion.TSDB_MIN_VERSION
}

func (rc *ReplicationContext) IsTSDB211GE() bool {
	return rc.tsdbVersion >= intversion.TSDB_211_VERSION
}

func (rc *ReplicationContext) IsLogicalReplicationEnabled() bool {
	return rc.walLevel == "logical"
}

// ----> SideChannel functions

func (rc *ReplicationContext) HasTablePrivilege(
	entity systemcatalog.SystemEntity, grant Grant) (access bool, err error) {

	return rc.sideChannel.hasTablePrivilege(rc.pgxConfig.User, entity, grant)
}

func (rc *ReplicationContext) LoadHypertables(cb func(hypertable *systemcatalog.Hypertable) error) error {
	return rc.sideChannel.readHypertables(cb)
}

func (rc *ReplicationContext) LoadChunks(cb func(chunk *systemcatalog.Chunk) error) error {
	return rc.sideChannel.readChunks(cb)
}

func (rc *ReplicationContext) ReadHypertableSchema(
	cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool,
	hypertables ...*systemcatalog.Hypertable) error {

	return rc.sideChannel.readHypertableSchema(cb, hypertables...)
}

func (rc *ReplicationContext) ExistsTableInPublication(entity systemcatalog.SystemEntity) (found bool, err error) {
	return rc.sideChannel.existsTableInPublication(rc.publicationName, entity.SchemaName(), entity.TableName())
}

func (rc *ReplicationContext) AttachTablesToPublication(entities ...systemcatalog.SystemEntity) error {
	return rc.sideChannel.attachTablesToPublication(rc.publicationName, entities...)
}

func (rc *ReplicationContext) DetachTablesFromPublication(entities ...systemcatalog.SystemEntity) error {
	return rc.sideChannel.detachTablesFromPublication(rc.publicationName, entities...)
}

func (rc *ReplicationContext) SnapshotTable(canonicalName string, snapshotName *string,
	cb func(lsn pgtypes.LSN, values map[string]any) error) (pgtypes.LSN, error) {

	return rc.sideChannel.snapshotTable(canonicalName, snapshotName, rc.snapshotBatchSize, cb)
}

func (rc *ReplicationContext) ReadReplicaIdentity(entity systemcatalog.SystemEntity) (pgtypes.ReplicaIdentity, error) {
	return rc.sideChannel.readReplicaIdentity(entity.SchemaName(), entity.TableName())
}

func (rc *ReplicationContext) ReadContinuousAggregate(
	materializedHypertableId int32) (viewSchema, viewName string, found bool, err error) {

	return rc.sideChannel.readContinuousAggregate(materializedHypertableId)
}

func (rc *ReplicationContext) ReadPublishedTables() ([]systemcatalog.SystemEntity, error) {
	return rc.sideChannel.readPublishedTables(rc.publicationName)
}

func (rc *ReplicationContext) CreatePublication() (bool, error) {
	return rc.sideChannel.createPublication(rc.publicationName)
}

func (rc *ReplicationContext) ExistsPublication() (bool, error) {
	return rc.sideChannel.existsPublication(rc.publicationName)
}

func (rc *ReplicationContext) DropPublication() error {
	return rc.sideChannel.dropPublication(rc.publicationName)
}

func (rc *ReplicationContext) GetSnapshotHighWatermark(hypertable *systemcatalog.Hypertable) (map[string]any, error) {
	return rc.sideChannel.getSnapshotHighWatermark(hypertable)
}

// ----> Dispatcher functions

func (rc *ReplicationContext) RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
	rc.dispatcher.RegisterReplicationEventHandler(handler)
}

func (rc *ReplicationContext) EnqueueTask(task Task) error {
	return rc.dispatcher.EnqueueTask(task)
}

func (rc *ReplicationContext) EnqueueTaskAndWait(task Task) error {
	return rc.dispatcher.EnqueueTaskAndWait(task)
}

// ----> Name Generator functions

func (rc *ReplicationContext) EventTopicName(hypertable *systemcatalog.Hypertable) string {
	return rc.namingStrategy.EventTopicName(rc.topicPrefix, hypertable)
}

func (rc *ReplicationContext) SchemaTopicName(hypertable *systemcatalog.Hypertable) string {
	return rc.namingStrategy.SchemaTopicName(rc.topicPrefix, hypertable)
}

func (rc *ReplicationContext) MessageTopicName() string {
	return rc.namingStrategy.MessageTopicName(rc.topicPrefix)
}

// ----> Name Generator functions

func (rc *ReplicationContext) RegisterSchema(schemaName string, schema schema.Struct) {
	rc.schemaRegistry.RegisterSchema(schemaName, schema)
}

func (rc *ReplicationContext) GetSchema(schemaName string) schema.Struct {
	return rc.schemaRegistry.GetSchema(schemaName)
}

func (rc *ReplicationContext) GetSchemaOrCreate(schemaName string, creator func() schema.Struct) schema.Struct {
	return rc.schemaRegistry.GetSchemaOrCreate(schemaName, creator)
}

func (rc *ReplicationContext) HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string {
	return rc.schemaRegistry.HypertableEnvelopeSchemaName(hypertable)
}

func (rc *ReplicationContext) HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string {
	return rc.schemaRegistry.HypertableKeySchemaName(hypertable)
}

func (rc *ReplicationContext) MessageEnvelopeSchemaName() string {
	return rc.schemaRegistry.MessageEnvelopeSchemaName()
}

func (rc *ReplicationContext) NewReplicationConnection() (*ReplicationConnection, error) {
	return newReplicationConnection(rc)
}

func (rc *ReplicationContext) newReplicationChannelConnection(ctx context.Context) (*pgconn.PgConn, error) {
	connConfig := rc.pgxConfig.Config.Copy()
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}
	connConfig.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, connConfig)
}

func (rc *ReplicationContext) newSideChannelConnection(ctx context.Context) (*pgx.Conn, error) {
	return pgx.ConnectConfig(ctx, rc.pgxConfig)
}

func (rc *ReplicationContext) setPositionLSNs(receivedLSN, processedLSN pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	rc.lastReceivedLSN = receivedLSN
	rc.lastProcessedLSN = processedLSN
}

func (rc *ReplicationContext) positionLSNs() (receivedLSN, processedLSN pgtypes.LSN) {
	rc.lsnMutex.Lock()
	defer rc.lsnMutex.Unlock()

	return rc.lastReceivedLSN, rc.lastProcessedLSN
}
