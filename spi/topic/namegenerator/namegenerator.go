package namegenerator

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

// NameGenerator wraps a namingstrategy.NamingStrategy to
// simplify its usage with the topic prefix being predefined
type NameGenerator interface {
	// EventTopicName generates a event topic name for the given hypertable
	EventTopicName(hypertable *systemcatalog.Hypertable) string
	// SchemaTopicName generates a schema topic name for the given hypertable
	SchemaTopicName(hypertable *systemcatalog.Hypertable) string
	// MessageTopicName generates a message topic name for a replication message
	MessageTopicName() string
}
