package context

import (
	"encoding"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes/datatypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
)

type ReplicationContext interface {
	StartReplicationContext() error
	StopReplicationContext() error
	NewReplicationConnection() (*ReplicationConnection, error)

	PublicationManager() PublicationManager
	StateManager() StateManager
	SchemaManager() SchemaManager
	TaskManager() TaskManager
	TypeManager() TypeManager

	Offset() (*statestorage.Offset, error)
	SetLastTransactionId(xid uint32)
	LastTransactionId() uint32
	SetLastBeginLSN(lsn pgtypes.LSN)
	LastBeginLSN() pgtypes.LSN
	SetLastCommitLSN(lsn pgtypes.LSN)
	LastCommitLSN() pgtypes.LSN
	AcknowledgeReceived(xld pgtypes.XLogData)
	LastReceivedLSN() pgtypes.LSN
	AcknowledgeProcessed(xld pgtypes.XLogData, processedLSN *pgtypes.LSN) error
	LastProcessedLSN() pgtypes.LSN

	InitialSnapshotMode() spiconfig.InitialSnapshotMode
	DatabaseUsername() string
	ReplicationSlotName() string
	ReplicationSlotCreate() bool
	ReplicationSlotAutoDrop() bool
	WALLevel() string
	SystemId() string
	Timeline() int32
	DatabaseName() string

	PostgresVersion() version.PostgresVersion
	TimescaleVersion() version.TimescaleVersion
	IsMinimumPostgresVersion() bool
	IsPG14GE() bool
	IsMinimumTimescaleVersion() bool
	IsTSDB212GE() bool
	IsLogicalReplicationEnabled() bool

	HasTablePrivilege(entity systemcatalog.SystemEntity, grant Grant) (access bool, err error)
	LoadHypertables(cb func(hypertable *systemcatalog.Hypertable) error) error
	LoadChunks(cb func(chunk *systemcatalog.Chunk) error) error
	ReadHypertableSchema(cb func(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool,
		hypertables ...*systemcatalog.Hypertable) error
	SnapshotChunkTable(chunk *systemcatalog.Chunk,
		cb func(lsn pgtypes.LSN, values map[string]any) error) (pgtypes.LSN, error)
	FetchHypertableSnapshotBatch(hypertable *systemcatalog.Hypertable, snapshotName string,
		cb func(lsn pgtypes.LSN, values map[string]any) error) error
	ReadSnapshotHighWatermark(hypertable *systemcatalog.Hypertable, snapshotName string) (map[string]any, error)
	ReadReplicaIdentity(entity systemcatalog.SystemEntity) (pgtypes.ReplicaIdentity, error)
	ReadContinuousAggregate(materializedHypertableId int32) (viewSchema, viewName string, found bool, err error)
}

type PublicationManager interface {
	PublicationName() string
	PublicationCreate() bool
	PublicationAutoDrop() bool
	CreatePublication() (bool, error)
	ExistsPublication() (bool, error)
	DropPublication() error
	ReadPublishedTables() ([]systemcatalog.SystemEntity, error)
	ExistsTableInPublication(entity systemcatalog.SystemEntity) (found bool, err error)
	AttachTablesToPublication(entities ...systemcatalog.SystemEntity) error
	DetachTablesFromPublication(entities ...systemcatalog.SystemEntity) error
}

type StateManager interface {
	StateEncoder(name string, encoder encoding.BinaryMarshaler) error
	StateDecoder(name string, decoder encoding.BinaryUnmarshaler) (present bool, err error)
	SetEncodedState(name string, encodedState []byte)
	EncodedState(name string) (encodedState []byte, present bool)
	SnapshotContext() (*watermark.SnapshotContext, error)
	SnapshotContextTransaction(snapshotName string, createIfNotExists bool,
		transaction func(snapshotContext *watermark.SnapshotContext) error) error
}

type SchemaManager interface {
	TopicPrefix() string
	EventTopicName(hypertable *systemcatalog.Hypertable) string
	SchemaTopicName(hypertable *systemcatalog.Hypertable) string
	MessageTopicName() string
	RegisterSchema(schemaName string, schema schemamodel.Struct)
	GetSchema(schemaName string) schemamodel.Struct
	GetSchemaOrCreate(schemaName string, creator func() schemamodel.Struct) schemamodel.Struct
	HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string
	HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string
	MessageEnvelopeSchemaName() string
}

type TaskManager interface {
	RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler)
	EnqueueTask(task Task) error
	RunTask(task Task) error
	EnqueueTaskAndWait(task Task) error
}

type TypeManager interface {
	DataType(oid uint32) (systemcatalog.PgType, error)
	Converter(oid uint32) (datatypes.Converter, error)
	NumKnownTypes() int
}
