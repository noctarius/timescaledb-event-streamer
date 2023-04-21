package context

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/namingstrategies"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReplicationContext_EventTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)

	replicationContext := &ReplicationContext{
		topicPrefix:    topicPrefix,
		namingStrategy: &namingstrategies.DebeziumNamingStrategy{},
	}

	topicName := replicationContext.EventTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)

}
func TestReplicationContext_SchemaTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)

	replicationContext := &ReplicationContext{
		topicPrefix:    topicPrefix,
		namingStrategy: &namingstrategies.DebeziumNamingStrategy{},
	}

	topicName := replicationContext.SchemaTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}
