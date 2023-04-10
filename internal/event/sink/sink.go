package sink

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/schema"
	"time"
)

type Sink interface {
	Emit(timestamp time.Time, topicName string, key, envelope schema.Struct) error
}
