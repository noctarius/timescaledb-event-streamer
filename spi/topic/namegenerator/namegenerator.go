package namegenerator

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type NameGenerator interface {
	EventTopicName(hypertable *systemcatalog.Hypertable) string
	SchemaTopicName(hypertable *systemcatalog.Hypertable) string
	MessageTopicName() string
}
