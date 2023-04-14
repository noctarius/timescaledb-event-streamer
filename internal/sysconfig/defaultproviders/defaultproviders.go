package defaultproviders

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

type EventEmitterProvider = func(
	config *spiconfig.Config, replicationContext *context.ReplicationContext, sinkProvider sink.Provider,
	transactionMonitor *transactional.TransactionMonitor) (*eventemitting.EventEmitter, error)

func DefaultSinkProvider(config *spiconfig.Config) (sink.Sink, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertySink, spiconfig.Stdout)
	return sink.NewSink(name, config)
}

func DefaultNamingStrategyProvider(config *spiconfig.Config) (namingstrategy.NamingStrategy, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertyNamingStrategy, spiconfig.Debezium)
	return namingstrategy.NewNamingStrategy(name, config)
}

func DefaultEventEmitterProvider(
	config *spiconfig.Config, replicationContext *context.ReplicationContext, sinkProvider sink.Provider,
	transactionMonitor *transactional.TransactionMonitor) (*eventemitting.EventEmitter, error) {

	s, err := sinkProvider(config)
	if err != nil {
		return nil, err
	}

	filters, err := eventfiltering.NewEventFilter(config.Sink.Filters)
	if err != nil {
		return nil, err
	}

	return eventemitting.NewEventEmitter(replicationContext, transactionMonitor, s, filters), nil
}
