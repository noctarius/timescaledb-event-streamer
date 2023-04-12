package namegenerator

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

type NameGenerator struct {
	topicPrefix         string
	topicNamingStrategy namingstrategy.NamingStrategy
}

func NewNameGenerator(topicPrefix string, topicNamingStrategy namingstrategy.NamingStrategy) *NameGenerator {
	return &NameGenerator{
		topicPrefix:         topicPrefix,
		topicNamingStrategy: topicNamingStrategy,
	}
}

func (ng *NameGenerator) EventTopicName(hypertable *systemcatalog.Hypertable) string {
	return ng.topicNamingStrategy.EventTopicName(ng.topicPrefix, hypertable)
}

func (ng *NameGenerator) SchemaTopicName(hypertable *systemcatalog.Hypertable) string {
	return ng.topicNamingStrategy.SchemaTopicName(ng.topicPrefix, hypertable)
}

func (ng *NameGenerator) MessageTopicName() string {
	return ng.topicNamingStrategy.MessageTopicName(ng.topicPrefix)
}
