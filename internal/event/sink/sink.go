package sink

import "github.com/noctarius/event-stream-prototype/internal/schema"

type Sink interface {
	Emit(topicName string, envelope schema.Struct) bool
}
