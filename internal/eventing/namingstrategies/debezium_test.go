package namingstrategies

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDebeziumNamingStrategy_EventTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)

	strategy := DebeziumNamingStrategy{}
	topicName := strategy.EventTopicName(topicPrefix, hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}

func TestDebeziumNamingStrategy_SchemaTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)

	strategy := DebeziumNamingStrategy{}
	topicName := strategy.SchemaTopicName(topicPrefix, hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}
