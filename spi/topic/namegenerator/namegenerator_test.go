package namegenerator

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/event/topic"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNameGenerator_EventTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil)

	nameGenerator := NewNameGenerator(topicPrefix, &topic.DebeziumNamingStrategy{})
	topicName := nameGenerator.EventTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)

}
func TestNameGenerator_SchemaTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil)

	nameGenerator := NewNameGenerator(topicPrefix, &topic.DebeziumNamingStrategy{})
	topicName := nameGenerator.SchemaTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}
