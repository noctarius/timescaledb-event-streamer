package defaultproviders

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

type EventEmitterProvider = func(
	config *spiconfig.Config, replicationContext *context.ReplicationContext, sink sink.Sink,
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
	config *spiconfig.Config, replicationContext *context.ReplicationContext, sink sink.Sink,
	transactionMonitor *transactional.TransactionMonitor) (*eventemitting.EventEmitter, error) {

	filters, err := eventfiltering.NewEventFilter(config.Sink.Filters)
	if err != nil {
		return nil, err
	}

	return eventemitting.NewEventEmitter(replicationContext, transactionMonitor, sink, filters), nil
}

func DefaultStateStorageProvider(config *spiconfig.Config) (statestorage.Storage, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertyStateStorageType, spiconfig.NoneStorage)
	return statestorage.NewStateStorage(name, config)
}
