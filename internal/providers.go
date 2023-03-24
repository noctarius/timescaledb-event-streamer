package internal

import (
	"fmt"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/kafka"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/nats"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/stdout"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/schema"
)

func newEventEmitter(config *configuration.Config, schemaRegistry *schema.Registry,
	topicNameGenerator *topic.NameGenerator) (*sink.EventEmitter, error) {

	s, err := newSink(config)
	if err != nil {
		return nil, err
	}

	return sink.NewEventEmitter(schemaRegistry, topicNameGenerator, s), nil
}

func newSink(config *configuration.Config) (sink.Sink, error) {
	switch configuration.GetOrDefault(config, "sink.type", configuration.Stdout) {
	case configuration.Stdout:
		return stdout.NewStdoutSink(), nil
	case configuration.NATS:
		return nats.NewNatsSink(config)
	case configuration.Kafka:
		return kafka.NewKafkaSink(config)
	}
	return nil, fmt.Errorf("SinkType '%s' doesn't exist", config.Sink.Type)
}

func newNameGenerator(config *configuration.Config) (*topic.NameGenerator, error) {
	namingStrategy, err := newNamingStrategy(config)
	if err != nil {
		return nil, err
	}
	return topic.NewNameGenerator(config.Topic.Prefix, namingStrategy), nil
}

func newNamingStrategy(config *configuration.Config) (topic.NamingStrategy, error) {
	switch configuration.GetOrDefault(config, "topic.namingstrategy.type", configuration.Debezium) {
	case configuration.Debezium:
		return &topic.DebeziumNamingStrategy{}, nil
	}
	return nil, fmt.Errorf("NamingStrategyType '%s' doesn't exist", config.Topic.NamingStrategy.Type)
}
