package stdout

import (
	"encoding/json"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"time"
)

var logger = logging.NewLogger("StdoutSink")

type stdoutSink struct{}

func NewStdoutSink() sink.Sink {
	return &stdoutSink{}
}

func (s *stdoutSink) Emit(_ time.Time, topicName string, _, envelope schema.Struct) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	logger.Printf("===> /%s: \t%s\n", topicName, string(data))
	return nil
}
