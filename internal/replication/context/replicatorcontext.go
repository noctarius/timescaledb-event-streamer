package context

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	intschema "github.com/noctarius/timescaledb-event-streamer/internal/eventing/schema"
	intversion "github.com/noctarius/timescaledb-event-streamer/internal/version"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/urfave/cli"
)

type ReplicationContext struct {
	sideChannel    *sideChannelImpl
	dispatcher     *dispatcher
	schemaRegistry schema.Registry
	namingStrategy namingstrategy.NamingStrategy

	snapshotBatchSize int
	publicationName   string
	topicPrefix       string

	timeline     int32
	systemId     string
	databaseName string
	walLevel     string

	pgVersion   version.PostgresVersion
	tsdbVersion version.TimescaleVersion
}

func NewReplicationContext(config *spiconfig.Config, pgxConfig *pgx.ConnConfig,
	namingStrategyProvider namingstrategy.Provider) (*ReplicationContext, error) {
	publicationName := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlPublicationName, "",
	)
	snapshotBatchSize := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlSnapshotBatchsize, 1000,
	)

	// Instantiate the actual side channel implementation
	// which handles queries against the database
	sideChannel, err := newSideChannel(pgxConfig)
	if err != nil {
		return nil, err
	}

	namingStrategy, err := namingStrategyProvider(config)
	if err != nil {
		return nil, err
	}

	pgVersion, err := sideChannel.getPostgresVersion()
	if err != nil {
		return nil, err
	}

	tsdbVersion, found, err := sideChannel.getTimescaleDBVersion()
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cli.NewExitError("TimescaleDB extension not found", 17)
	}

	databaseName, systemId, timeline, err := sideChannel.getSystemInformation()
	if err != nil {
		return nil, err
	}

	walLevel, err := sideChannel.getWalLevel()
	if err != nil {
		return nil, err
	}

	// Build the replication context to be passed along in terms of
	// potential interface implementations to break up internal dependencies
	replicationContext := &ReplicationContext{
		sideChannel:    sideChannel,
		dispatcher:     newDispatcher(),
		namingStrategy: namingStrategy,

		snapshotBatchSize: snapshotBatchSize,
		publicationName:   publicationName,
		topicPrefix:       config.Topic.Prefix,

		timeline:     timeline,
		systemId:     systemId,
		databaseName: databaseName,
		walLevel:     walLevel,
		pgVersion:    pgVersion,
		tsdbVersion:  tsdbVersion,
	}

	// Instantiate the schema registry, keeping track of hypertable schemas
	// for the schema generation on event creation
	replicationContext.schemaRegistry = intschema.NewRegistry(replicationContext)

	return replicationContext, nil
}

func (rp *ReplicationContext) StartReplicationContext() error {
	rp.dispatcher.StartDispatcher()
	return nil
}

func (rp *ReplicationContext) StopReplicationContext() error {
	return rp.dispatcher.StopDispatcher()
}

func (rp *ReplicationContext) PublicationName() string {
	return rp.publicationName
}

func (rp *ReplicationContext) WALLevel() string {
	return rp.walLevel
}

func (rp *ReplicationContext) SystemId() string {
	return rp.systemId
}

func (rp *ReplicationContext) Timeline() int32 {
	return rp.timeline
}

func (rp *ReplicationContext) DatabaseName() string {
	return rp.databaseName
}

func (rp *ReplicationContext) TopicPrefix() string {
	return rp.topicPrefix
}

func (rp *ReplicationContext) PostgresVersion() version.PostgresVersion {
	return rp.pgVersion
}

func (rp *ReplicationContext) TimescaleVersion() version.TimescaleVersion {
	return rp.tsdbVersion
}

func (rp *ReplicationContext) IsMinimumPostgresVersion() bool {
	return rp.pgVersion >= intversion.PG_MIN_VERSION
}

func (rp *ReplicationContext) IsPG14GE() bool {
	return rp.pgVersion >= intversion.PG_14_VERSION
}

func (rp *ReplicationContext) IsMinimumTimescaleVersion() bool {
	return rp.tsdbVersion >= intversion.TSDB_MIN_VERSION
}

func (rp *ReplicationContext) IsTSDB211GE() bool {
	return rp.tsdbVersion >= intversion.TSDB_211_VERSION
}

func (rp *ReplicationContext) IsLogicalReplicationEnabled() bool {
	return rp.walLevel == "logical"
}

// ----> SideChannel functions

func (rp *ReplicationContext) LoadHypertables(cb func(hypertable *systemcatalog.Hypertable) error) error {
	return rp.sideChannel.readHypertables(cb)
}

func (rp *ReplicationContext) LoadChunks(cb func(chunk *systemcatalog.Chunk) error) error {
	return rp.sideChannel.readChunks(cb)
}

func (rp *ReplicationContext) ReadHypertableSchema(
	cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool,
	hypertables ...*systemcatalog.Hypertable) error {

	return rp.sideChannel.readHypertableSchema(cb, hypertables...)
}

func (rp *ReplicationContext) AttachTablesToPublication(entities ...systemcatalog.SystemEntity) error {
	return rp.sideChannel.attachTablesToPublication(rp.publicationName, entities...)
}

func (rp *ReplicationContext) DetachTablesFromPublication(entities ...systemcatalog.SystemEntity) error {
	return rp.sideChannel.detachTablesFromPublication(rp.publicationName, entities...)
}

func (rp *ReplicationContext) SnapshotTable(canonicalName string, startingLSN *pglogrepl.LSN,
	cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error) {

	return rp.sideChannel.snapshotTable(canonicalName, startingLSN, rp.snapshotBatchSize, cb)
}

func (rp *ReplicationContext) ReadReplicaIdentity(entity systemcatalog.SystemEntity) (pgtypes.ReplicaIdentity, error) {
	return rp.sideChannel.readReplicaIdentity(entity.SchemaName(), entity.TableName())
}

func (rp *ReplicationContext) ReadContinuousAggregate(
	materializedHypertableId int32) (viewSchema, viewName string, found bool, err error) {

	return rp.sideChannel.readContinuousAggregate(materializedHypertableId)
}

func (rp *ReplicationContext) ReadPublishedTables() ([]systemcatalog.SystemEntity, error) {
	return rp.sideChannel.readPublishedTables(rp.publicationName)
}

func (rp *ReplicationContext) CreatePublication() (bool, error) {
	return rp.sideChannel.createPublication(rp.publicationName)
}

func (rp *ReplicationContext) ExistsPublication() (bool, error) {
	return rp.sideChannel.existsPublication(rp.publicationName)
}

func (rp *ReplicationContext) DropPublication() error {
	return rp.sideChannel.dropPublication(rp.publicationName)
}

// ----> Dispatcher functions

func (rp *ReplicationContext) RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
	rp.dispatcher.RegisterReplicationEventHandler(handler)
}

func (rp *ReplicationContext) EnqueueTask(task Task) error {
	return rp.dispatcher.EnqueueTask(task)
}

func (rp *ReplicationContext) EnqueueTaskAndWait(task Task) error {
	return rp.dispatcher.EnqueueTaskAndWait(task)
}

// ----> Name Generator functions

func (rp *ReplicationContext) EventTopicName(hypertable *systemcatalog.Hypertable) string {
	return rp.namingStrategy.EventTopicName(rp.topicPrefix, hypertable)
}

func (rp *ReplicationContext) SchemaTopicName(hypertable *systemcatalog.Hypertable) string {
	return rp.namingStrategy.SchemaTopicName(rp.topicPrefix, hypertable)
}

func (rp *ReplicationContext) MessageTopicName() string {
	return rp.namingStrategy.MessageTopicName(rp.topicPrefix)
}

// ----> Name Generator functions

func (rp *ReplicationContext) RegisterSchema(schemaName string, schema schema.Struct) {
	rp.schemaRegistry.RegisterSchema(schemaName, schema)
}

func (rp *ReplicationContext) GetSchema(schemaName string) schema.Struct {
	return rp.schemaRegistry.GetSchema(schemaName)
}

func (rp *ReplicationContext) GetSchemaOrCreate(schemaName string, creator func() schema.Struct) schema.Struct {
	return rp.schemaRegistry.GetSchemaOrCreate(schemaName, creator)
}

func (rp *ReplicationContext) HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string {
	return rp.schemaRegistry.HypertableEnvelopeSchemaName(hypertable)
}

func (rp *ReplicationContext) HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string {
	return rp.schemaRegistry.HypertableKeySchemaName(hypertable)
}

func (rp *ReplicationContext) MessageEnvelopeSchemaName() string {
	return rp.schemaRegistry.MessageEnvelopeSchemaName()
}
