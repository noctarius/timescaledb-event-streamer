package topic

import (
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNameGenerator_EventTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := model.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil)

	nameGenerator := NewNameGenerator(topicPrefix, &DebeziumNamingStrategy{})
	topicName := nameGenerator.EventTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)

}
func TestNameGenerator_SchemaTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := model.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil)

	nameGenerator := NewNameGenerator(topicPrefix, &DebeziumNamingStrategy{})
	topicName := nameGenerator.SchemaTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}
