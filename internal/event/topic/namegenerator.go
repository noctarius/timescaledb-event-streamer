package topic

import "github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"

type NameGenerator struct {
	topicPrefix         string
	topicNamingStrategy NamingStrategy
}

func NewNameGenerator(topicPrefix string, topicNamingStrategy NamingStrategy) *NameGenerator {
	return &NameGenerator{
		topicPrefix:         topicPrefix,
		topicNamingStrategy: topicNamingStrategy,
	}
}

func (ng *NameGenerator) EventTopicName(hypertable *model.Hypertable) string {
	return ng.topicNamingStrategy.EventTopicName(ng.topicPrefix, hypertable)
}

func (ng *NameGenerator) SchemaTopicName(hypertable *model.Hypertable) string {
	return ng.topicNamingStrategy.SchemaTopicName(ng.topicPrefix, hypertable)
}
