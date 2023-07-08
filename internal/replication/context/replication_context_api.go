package context

import (
	"encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
)

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
	RegisterSchema(schemaName string, schema schema.Struct)
	GetSchema(schemaName string) schema.Struct
	GetSchemaOrCreate(schemaName string, creator func() schema.Struct) schema.Struct
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
