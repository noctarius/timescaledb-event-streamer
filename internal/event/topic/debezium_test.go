package topic

import (
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDebeziumNamingStrategy_EventTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := model.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil)

	strategy := DebeziumNamingStrategy{}
	topicName := strategy.EventTopicName(topicPrefix, hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}

func TestDebeziumNamingStrategy_SchemaTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := model.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil)

	strategy := DebeziumNamingStrategy{}
	topicName := strategy.SchemaTopicName(topicPrefix, hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}
