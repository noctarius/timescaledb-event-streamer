package sink

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"time"
)

type Provider = func(config *config.Config) (Sink, error)

type Sink interface {
	Emit(timestamp time.Time, topicName string, key, envelope schema.Struct) error
}
