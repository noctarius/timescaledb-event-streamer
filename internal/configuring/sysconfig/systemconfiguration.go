package sysconfig

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/event/eventfiltering"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/kafka"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/nats"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/redis"
	"github.com/noctarius/event-stream-prototype/internal/event/sink/stdout"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/replication/transactional"
	"github.com/noctarius/event-stream-prototype/internal/schema"
)

type SystemConfig struct {
	*configuring.Config

	PgxConfig              *pgx.ConnConfig
	SinkProvider           SinkProvider
	EventEmitterProvider   EventEmitterProvider
	NameGeneratorProvider  NameGeneratorProvider
	NamingStrategyProvider NamingStrategyProvider
}

func NewSystemConfig(config *configuring.Config) *SystemConfig {
	sc := &SystemConfig{
		Config: config,
	}
	sc.SinkProvider = sc.defaultSink
	sc.EventEmitterProvider = sc.defaultEventEmitter
	sc.NameGeneratorProvider = sc.defaultNameGenerator
	sc.NamingStrategyProvider = sc.defaultNamingStrategy
	return sc
}

func (sc *SystemConfig) defaultEventEmitter(schemaRegistry *schema.Registry, topicNameGenerator *topic.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor) (*sink.EventEmitter, error) {

	s, err := sc.SinkProvider()
	if err != nil {
		return nil, err
	}

	filters, err := eventfiltering.NewSinkEventFilter(sc.Config)
	if err != nil {
		return nil, err
	}

	return sink.NewEventEmitter(schemaRegistry, topicNameGenerator, transactionMonitor, s, filters), nil
}

func (sc *SystemConfig) defaultSink() (sink.Sink, error) {
	switch configuring.GetOrDefault(sc.Config, "sink.type", configuring.Stdout) {
	case configuring.Stdout:
		return stdout.NewStdoutSink(), nil
	case configuring.NATS:
		return nats.NewNatsSink(sc.Config)
	case configuring.Kafka:
		return kafka.NewKafkaSink(sc.Config)
	case configuring.Redis:
		return redis.NewRedisSink(sc.Config)
	}
	return nil, fmt.Errorf("SinkType '%s' doesn't exist", sc.Config.Sink.Type)
}

func (sc *SystemConfig) defaultNameGenerator() (*topic.NameGenerator, error) {
	namingStrategy, err := sc.NamingStrategyProvider()
	if err != nil {
		return nil, err
	}
	return topic.NewNameGenerator(sc.Config.Topic.Prefix, namingStrategy), nil
}

func (sc *SystemConfig) defaultNamingStrategy() (topic.NamingStrategy, error) {
	switch configuring.GetOrDefault(sc.Config, "topic.namingstrategy.type", configuring.Debezium) {
	case configuring.Debezium:
		return &topic.DebeziumNamingStrategy{}, nil
	}
	return nil, fmt.Errorf("NamingStrategyType '%s' doesn't exist", sc.Config.Topic.NamingStrategy.Type)
}
