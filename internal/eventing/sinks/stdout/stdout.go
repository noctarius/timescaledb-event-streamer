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

func newStdoutSink(_ *spiconfig.Config) (sink.Sink, error) {
	return sink.SinkFunc(
		func(_ sink.Context, _ time.Time, _ string, _, envelope schema.Struct) error {
			delete(envelope, "schema")
			data, err := json.Marshal(envelope)
			if err != nil {
				return err
			}
			_, err = os.Stdout.WriteString(fmt.Sprintf("%s\n", string(data)))
			return err
		},
	), nil
}
