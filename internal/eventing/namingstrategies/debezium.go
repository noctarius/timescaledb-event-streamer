package namingstrategies

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

func init() {
	namingstrategy.RegisterNamingStrategy(config.Debezium,
		func(_ *config.Config) (namingstrategy.NamingStrategy, error) {
			return &DebeziumNamingStrategy{}, nil
		},
	)
}

type DebeziumNamingStrategy struct {
}

func (d *DebeziumNamingStrategy) EventTopicName(topicPrefix string, hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.%s.%s", topicPrefix, hypertable.SchemaName(), hypertable.HypertableName())
}

func (d *DebeziumNamingStrategy) SchemaTopicName(topicPrefix string, hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.%s.%s", topicPrefix, hypertable.SchemaName(), hypertable.HypertableName())
}

func (d *DebeziumNamingStrategy) MessageTopicName(topicPrefix string) string {
	return fmt.Sprintf("%s.message", topicPrefix)
}
