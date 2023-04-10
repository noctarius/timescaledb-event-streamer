package stdout

import (
	"encoding/json"
	"github.com/noctarius/timescaledb-event-streamer/internal/event/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/schema"
	"time"
)

var logger = logging.NewLogger("StdoutSink")

type stdoutSink struct{}

func NewStdoutSink() sink.Sink {
	return &stdoutSink{}
}

func (s *stdoutSink) Emit(_ time.Time, topicName string, _, envelope schema.Struct) error {
	delete(envelope, "schema")
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	logger.Printf("===> /%s: \t%s\n", topicName, string(data))
	return nil
}
