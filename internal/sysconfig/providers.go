package sysconfig

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
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

type TypeManagerProvider = func(sideChannel sidechannel.SideChannel) (pgtypes.TypeManager, error)

type SinkManagerProvider = func(config *config.Config, stateStorageManager statestorage.Manager) (sink.Manager, error)

type SnapshotterProvider = func(
	partitionCount int, replicationContext replicationcontext.ReplicationContext,
) (*snapshotting.Snapshotter, error)

type ReplicationChannelProvider = func(
	replicationContext replicationcontext.ReplicationContext,
) (*replicationchannel.ReplicationChannel, error)

type NameGeneratorProvider = func(
	config *config.Config, namingStrategy namingstrategy.NamingStrategy,
) schema.NameGenerator

type SideChannelProvider = func(
	stateStorageManager statestorage.Manager, pgxConfig *pgx.ConnConfig,
) (sidechannel.SideChannel, error)

type StateStorageManagerProvider = func(config *config.Config) (statestorage.Manager, error)

type ReplicationContextProvider func(
	config *config.Config, pgxConfig *pgx.ConnConfig,
	stateStorageManager statestorage.Manager, sideChannel sidechannel.SideChannel,
) (replicationcontext.ReplicationContext, error)

type LogicalReplicationResolverProvider = func(
	c *config.Config, replicationContext replicationcontext.ReplicationContext,
	systemCatalog *systemcatalog.SystemCatalog,
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
