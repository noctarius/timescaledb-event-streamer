package sysconfig

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
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
)

var (
	_ = TypeManagerProvider(pgtypes.NewTypeManager)
	_ = SinkManagerProvider(sink.NewSinkManager)
	_ = SnapshotterProvider(snapshotting.NewSnapshotterFromConfig)
	_ = ReplicationChannelProvider(replicationchannel.NewReplicationChannel)
	_ = NameGeneratorProvider(schema.NewNameGeneratorFromConfig)
	_ = SideChannelProvider(sidechannel.NewSideChannel)
	_ = StateStorageManagerProvider(statestorage.NewStateStorageManager)
	_ = ReplicationContextProvider(replicationcontext.NewReplicationContext)
	_ = LogicalReplicationResolverProvider(logicalreplicationresolver.NewResolver)
	_ = StreamManagerProvider(stream.NewStreamManager)
	_ = SystemCatalogProvider(systemcatalog.NewSystemCatalog)
	_ = EventEmitterProvider(eventemitting.NewEventEmitterFromConfig)
)

type TypeManagerProvider = func(typeResolver pgtypes.TypeResolver) (pgtypes.TypeManager, error)

type SinkManagerProvider = func(stateStorageManager statestorage.Manager, sink sink.Sink) sink.Manager

type SnapshotterProvider = func(
	c *config.Config, replicationContext replicationcontext.ReplicationContext,
) (*snapshotting.Snapshotter, error)

type ReplicationChannelProvider = func(
	replicationContext replicationcontext.ReplicationContext, typeManager pgtypes.TypeManager,
) (*replicationchannel.ReplicationChannel, error)

type NameGeneratorProvider = func(
	config *config.Config, namingStrategy namingstrategy.NamingStrategy,
) schema.NameGenerator

type SideChannelProvider = func(
	stateStorageManager statestorage.Manager, pgxConfig *pgx.ConnConfig,
) (sidechannel.SideChannel, error)

type StateStorageManagerProvider = func(stateStorage statestorage.Storage) statestorage.Manager

type ReplicationContextProvider func(
	config *config.Config, pgxConfig *pgx.ConnConfig,
	stateStorageManager statestorage.Manager, sideChannel sidechannel.SideChannel,
) (replicationcontext.ReplicationContext, error)

type LogicalReplicationResolverProvider = func(
	c *config.Config, replicationContext replicationcontext.ReplicationContext,
	systemCatalog *systemcatalog.SystemCatalog, typeManager pgtypes.TypeManager,
) (eventhandlers.BaseReplicationEventHandler, error)

type StreamManagerProvider = func(
	nameGenerator schema.NameGenerator, typeManager pgtypes.TypeManager, sinkManager sink.Manager,
) (stream.Manager, error)

type SystemCatalogProvider = func(
	config *config.Config, replicationContext replicationcontext.ReplicationContext,
	pgTypeResolver func(oid uint32) (pgtypes.PgType, error), snapshotter *snapshotting.Snapshotter,
) (*systemcatalog.SystemCatalog, error)

type EventEmitterProvider = func(
	config *config.Config, replicationContext replicationcontext.ReplicationContext,
	streamManager stream.Manager, typeManager pgtypes.TypeManager,
) (*eventemitting.EventEmitter, error)
