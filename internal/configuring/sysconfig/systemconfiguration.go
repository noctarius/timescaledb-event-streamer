package sysconfig

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/event/eventfiltering"
	intsink "github.com/noctarius/timescaledb-event-streamer/internal/event/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namegenerator"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

type SystemConfig struct {
	*spiconfig.Config

	PgxConfig              *pgx.ConnConfig
	SinkProvider           sink.Provider
	EventEmitterProvider   EventEmitterProvider
	NameGeneratorProvider  NameGeneratorProvider
	NamingStrategyProvider namingstrategy.Provider
}

func NewSystemConfig(config *spiconfig.Config) *SystemConfig {
	sc := &SystemConfig{
		Config: config,
	}
	sc.SinkProvider = sc.defaultSink
	sc.EventEmitterProvider = sc.defaultEventEmitter
	sc.NameGeneratorProvider = sc.defaultNameGenerator
	sc.NamingStrategyProvider = sc.defaultNamingStrategy
	return sc
}

func (sc *SystemConfig) defaultEventEmitter(schemaRegistry *schema.Registry, topicNameGenerator *namegenerator.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor) (*intsink.EventEmitter, error) {

	s, err := sc.SinkProvider(sc.Config)
	if err != nil {
		return nil, err
	}

	filters, err := eventfiltering.NewSinkEventFilter(sc.Config.Sink.Filters)
	if err != nil {
		return nil, err
	}

	return intsink.NewEventEmitter(schemaRegistry, topicNameGenerator, transactionMonitor, s, filters), nil
}

func (sc *SystemConfig) defaultSink(config *spiconfig.Config) (sink.Sink, error) {
	name := spiconfig.GetOrDefault(config, "sink.type", spiconfig.Stdout)
	return sink.NewSink(name, config)
}

func (sc *SystemConfig) defaultNameGenerator() (*namegenerator.NameGenerator, error) {
	namingStrategy, err := sc.NamingStrategyProvider(sc.Config)
	if err != nil {
		return nil, err
	}
	return namegenerator.NewNameGenerator(sc.Config.Topic.Prefix, namingStrategy), nil
}

func (sc *SystemConfig) defaultNamingStrategy(config *spiconfig.Config) (namingstrategy.NamingStrategy, error) {
	name := spiconfig.GetOrDefault(config, "topic.namingstrategy.type", spiconfig.Debezium)
	return namingstrategy.NewNamingStrategy(name, config)
}
