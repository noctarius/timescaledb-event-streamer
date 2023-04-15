package stdout

import (
	"encoding/json"
	"fmt"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"os"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.Stdout, newStdoutSink)
}

type stdoutSink struct {
}

func newStdoutSink(_ *spiconfig.Config) (sink.Sink, error) {
	return &stdoutSink{}, nil
}

func (s *stdoutSink) Emit(_ time.Time, topicName string, _, envelope schema.Struct) error {
	delete(envelope, "schema")
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = os.Stdout.WriteString(fmt.Sprintf("===> /%s: \t%s\n", topicName, string(data)))
	return err
}
