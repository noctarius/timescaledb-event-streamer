package schema

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type Registry interface {
	RegisterSchema(schemaName string, schema Struct)
	GetSchema(schemaName string) Struct
	GetSchemaOrCreate(schemaName string, creator func() Struct) Struct
	HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string
	HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string
	MessageEnvelopeSchemaName() string
}
