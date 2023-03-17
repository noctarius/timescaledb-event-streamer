package stdout

import (
	"encoding/json"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/schema"
)

var logger = logging.NewLogger("StdoutSink")

type stdoutSink struct{}

func NewStdoutSink() sink.Sink {
	return &stdoutSink{}
}

func (s *stdoutSink) Emit(topicName string, envelope schema.Struct) bool {
	data, err := json.Marshal(envelope)
	if err != nil {
		return false
	}
	logger.Printf("===> /%s: \t%s\n", topicName, string(data))
	return true
}
