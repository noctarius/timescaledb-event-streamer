package replication

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationchannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/stream"
	"github.com/noctarius/timescaledb-event-streamer/spi/wiring"
)

var EventingModule = wiring.DefineModule(
	"Eventing", func(module wiring.Module) {
		module.Provide(func(
			c *config.Config, replicationContext replicationcontext.ReplicationContext,
			streamManager stream.Manager, typeManager pgtypes.TypeManager,
		) (*eventemitting.EventEmitter, error) {

			filters, err := eventfiltering.NewEventFilter(c.Sink.Filters)
			if err != nil {
				return nil, err
			}

			return eventemitting.NewEventEmitter(replicationContext, streamManager, typeManager, filters)
		})
	},
)

var StateStorageModule = wiring.DefineModule(
	"StateStorage",
	func(module wiring.Module) {
		module.Provide(func(c *config.Config) (statestorage.Storage, error) {
			name := config.GetOrDefault(c, config.PropertyStateStorageType, config.NoneStorage)
			return statestorage.NewStateStorage(name, c)
		})

		module.Provide(statestorage.NewStateStorageManager)
	},
)

var SideChannelModule = wiring.DefineModule(
	"SideChannel", func(module wiring.Module) {
		module.Provide(
			func(stateStorageManager statestorage.Manager, pgxConfig *pgx.ConnConfig) (sidechannel.SideChannel, error) {
				return sidechannel.NewSideChannel(stateStorageManager, pgxConfig)
			},
		)
	},
)

var TypeManagerModule = wiring.DefineModule(
	"TypeManager", func(module wiring.Module) {
		module.Provide(func(sideChannel sidechannel.SideChannel) (pgtypes.TypeManager, error) {
			// Necessary since TypeManager is looking for TypeResolver,
			// not SideChannel (which implements the interface)
			return pgtypes.NewTypeManager(sideChannel)
		})
	},
)

var ReplicationContextModule = wiring.DefineModule(
	"ReplicationContext", func(module wiring.Module) {

		module.Provide(func(
			c *config.Config, pgc *pgx.ConnConfig,
			stateStorageManager statestorage.Manager,
			sideChannel sidechannel.SideChannel,
		) (replicationcontext.ReplicationContext, error) {

			return replicationcontext.NewReplicationContext(c, pgc, stateStorageManager, sideChannel)
		})

	},
)

var LogicalReplicationResolverModule = wiring.DefineModule(
	"LogicalReplicationResolver", func(module wiring.Module) {
		module.Provide(func(
			c *config.Config, replicationContext replicationcontext.ReplicationContext, systemCatalog *systemcatalog.SystemCatalog,
		) (eventhandlers.BaseReplicationEventHandler, error) {

			return logicalreplicationresolver.NewResolver(c, replicationContext, systemCatalog)
		}, wiring.ForceInitialization())
	},
)

var SchemaModule = wiring.DefineModule(
	"Schema", func(module wiring.Module) {
		module.Provide(func(config *config.Config, namingStrategy namingstrategy.NamingStrategy) schema.NameGenerator {
			return schema.NewNameGenerator(config.Topic.Prefix, namingStrategy)
		})
	},
)

var NamingStrategyModule = wiring.DefineModule(
	"NamingStrategy", func(module wiring.Module) {
		module.Provide(func(c *config.Config) (namingstrategy.NamingStrategy, error) {
			name := config.GetOrDefault(c, config.PropertyNamingStrategy, config.Debezium)
			return namingstrategy.NewNamingStrategy(name, c)
		})
	},
)

var ReplicationChannelModule = wiring.DefineModule(
	"ReplicationChannel", func(module wiring.Module) {
		module.Provide(
			func(
				replicationContext replicationcontext.ReplicationContext,
			) (*replicationchannel.ReplicationChannel, error) {

				return replicationchannel.NewReplicationChannel(replicationContext)
			},
		)
	},
)

var SinkManagerModule = wiring.DefineModule(
	"Sink", func(module wiring.Module) {
		module.Provide(func(c *config.Config) (sink.Sink, error) {
			name := config.GetOrDefault(c, config.PropertySink, config.Stdout)
			return sink.NewSink(name, c)
		})

		module.Provide(sink.NewSinkManager)
	},
)

var StreamManagerModule = wiring.DefineModule(
	"StreamManager", func(module wiring.Module) {
		module.Provide(stream.NewStreamManager)
	},
)

var SystemCatalogModule = wiring.DefineModule(
	"SystemCatalog", func(module wiring.Module) {
		module.Provide(func(
			c *config.Config, replicationContext replicationcontext.ReplicationContext,
			typeManager pgtypes.TypeManager, snapshotter *snapshotting.Snapshotter,
		) (*systemcatalog.SystemCatalog, error) {

			return systemcatalog.NewSystemCatalog(c, replicationContext, typeManager.DataType, snapshotter)
		})
	},
)

var SnapshotterModule = wiring.DefineModule(
	"Snapshotter", func(module wiring.Module) {
		module.Provide(
			func(
				c *config.Config, replicationContext replicationcontext.ReplicationContext,
			) (*snapshotting.Snapshotter, error) {

				parallelism := config.GetOrDefault(c, config.PropertySnapshotterParallelism, uint8(5))
				return snapshotting.NewSnapshotter(parallelism, replicationContext)
			},
		)
	},
)
