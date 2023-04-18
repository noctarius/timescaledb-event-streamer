package sink

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"time"
)

type Provider = func(config *config.Config) (Sink, error)

type Sink interface {
	Emit(context Context, timestamp time.Time, topicName string, key, envelope schema.Struct) error
}

type SinkFunc func(context Context, timestamp time.Time, topicName string, key, envelope schema.Struct) error

func (sf SinkFunc) Emit(context Context, timestamp time.Time, topicName string, key, envelope schema.Struct) error {
	return sf(context, timestamp, topicName, key, envelope)
}

type Context interface {
	SetTransientAttribute(key string, value string)
	TransientAttribute(key string) (value string, present bool)
	SetAttribute(key string, value string)
	Attribute(key string) (value string, present bool)
}
