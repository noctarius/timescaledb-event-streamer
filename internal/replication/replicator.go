package replication

import (
	stderrors "errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventhandler"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/channels"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	"github.com/noctarius/timescaledb-event-streamer/internal/schema"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/internal/version"
	"github.com/urfave/cli"
)

type Replicator interface {
	StartReplication() *cli.ExitError

	StopReplication() *cli.ExitError
}

type replicatorImpl struct {
	config       *sysconfig.SystemConfig
	shutdownTask func() error
}

func NewReplicator(config *sysconfig.SystemConfig) Replicator {
	return &replicatorImpl{
		config: config,
	}
}

func (r *replicatorImpl) StartReplication() *cli.ExitError {
	config := r.config.Config

	publicationName := configuring.GetOrDefault(config, "postgresql.publication.name", "")
	snapshotBatchSize := configuring.GetOrDefault(config, "postgresql.snapshot.batchsize", 1000)

	// Create the side and replication channels
	sideChannel := channels.NewSideChannel(r.config.PgxConfig, publicationName, snapshotBatchSize)
	replicationChannel := channels.NewReplicationChannel(r.config.PgxConfig, publicationName)

	// Read version information
	pgVersion, err := sideChannel.GetPostgresVersion()
	if err != nil {
		return supporting.AdaptError(err, 11)
	}

	if pgVersion < version.PG_MIN_VERSION {
		return cli.NewExitError("timescaledb-event-streamer requires PostgreSQL 14 or later", 11)
	}

	tsdbVersion, err := sideChannel.GetTimescaleDBVersion()
	if err != nil {
		return supporting.AdaptError(err, 12)
	}

	if tsdbVersion < version.TSDB_MIN_VERSION {
		return cli.NewExitError("timescaledb-event-streamer requires TimescaleDB 2.10 or later", 12)
	}

	// Instantiate the event dispatcher
	dispatcher := eventhandler.NewDispatcher()

	// Instantiate the snapshotter
	snapshotter := snapshotting.NewSnapshotter(32, sideChannel, dispatcher)

	// Instantiate the topic name generator
	topicNameGenerator, err := r.config.NameGeneratorProvider()
	if err != nil {
		return supporting.AdaptError(err, 13)
	}

	// Instantiate the transaction monitor, keeping track of transaction boundaries
	transactionMonitor := transactional.NewTransactionMonitor()

	// Instantiate the schema registry, keeping track of hypertable schemata
	schemaRegistry := schema.NewSchemaRegistry(topicNameGenerator)

	// Instantiate the change event emitter
	eventEmitter, err := r.config.EventEmitterProvider(schemaRegistry, topicNameGenerator, transactionMonitor)
	if err != nil {
		return supporting.AdaptError(err, 14)
	}

	// Set up the system catalog (replicating the Timescale internal representation)
	systemCatalog, err := systemcatalog.NewSystemCatalog(
		r.config, topicNameGenerator, dispatcher, sideChannel, schemaRegistry, snapshotter,
	)
	if err != nil {
		return supporting.AdaptError(err, 15)
	}

	// Set up the internal transaction tracking and logical replication resolving
	transactionResolver := logicalreplicationresolver.NewTransactionResolver(r.config, dispatcher, systemCatalog)

	// Register event handlers
	dispatcher.RegisterReplicationEventHandler(transactionMonitor)
	dispatcher.RegisterReplicationEventHandler(transactionResolver)
	dispatcher.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	dispatcher.RegisterReplicationEventHandler(eventEmitter.NewEventHandler())

	// Start dispatching events
	dispatcher.StartDispatcher()

	// Start the snapshotter
	snapshotter.StartSnapshotter()

	// Get initial list of chunks to add to publication
	initialChunkTables := systemCatalog.GetAllChunks()

	// Filter chunks by already published tables
	alreadyPublished, err := sideChannel.ReadPublishedTables(publicationName)
	if err != nil {
		return supporting.AdaptError(err, 250)
	}
	initialChunkTables = supporting.Filter(initialChunkTables, func(item string) bool {
		return !supporting.Contains(alreadyPublished, item)
	})

	if err := replicationChannel.StartReplicationChannel(dispatcher, initialChunkTables); err != nil {
		return supporting.AdaptError(err, 16)
	}

	r.shutdownTask = func() error {
		snapshotter.StopSnapshotter()
		err1 := replicationChannel.StopReplicationChannel()
		err2 := dispatcher.StopDispatcher()
		return stderrors.Join(err1, err2)
	}

	return nil
}

func (r *replicatorImpl) StopReplication() *cli.ExitError {
	if r.shutdownTask != nil {
		return supporting.AdaptError(r.shutdownTask(), 250)
	}
	return nil
}
