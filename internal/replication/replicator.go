package replication

import (
	stderrors "errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	logrepresolver "github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationchannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	intsystemcatalog "github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/urfave/cli"
)

// Replicator is the main controller for all things logical replication,
// such as the logical replication connection, the side channel connection,
// and other services necessary to run the event stream generation.
type Replicator struct {
	logger       *logging.Logger
	config       *sysconfig.SystemConfig
	shutdownTask func() error
}

// NewReplicator instantiates a new instance of the Replicator.
func NewReplicator(config *sysconfig.SystemConfig) (*Replicator, error) {
	logger, err := logging.NewLogger("Replicator")
	if err != nil {
		return nil, err
	}

	return &Replicator{
		logger: logger,
		config: config,
	}, nil
}

// StartReplication initiates the actual replication process
func (r *Replicator) StartReplication() *cli.ExitError {
	namingStrategy, err := r.config.NamingStrategyProvider(r.config.Config)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate naming strategy", 21)
	}

	stateStorage, err := r.config.StateStorageProvider(r.config.Config)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate state storage", 23)
	}

	// Create the side channels and replication context
	replicationContext, err := context.NewReplicationContext(
		r.config.Config, r.config.PgxConfig, namingStrategy, stateStorage,
	)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to initialize replication context", 17)
	}

	// Check version information
	if !replicationContext.IsMinimumPostgresVersion() {
		return cli.NewExitError("timescaledb-event-streamer requires PostgreSQL 14 or later", 11)
	}
	if !replicationContext.IsMinimumTimescaleVersion() {
		return cli.NewExitError("timescaledb-event-streamer requires TimescaleDB 2.10 or later", 12)
	}

	// Check WAL replication level
	if !replicationContext.IsLogicalReplicationEnabled() {
		return cli.NewExitError("timescaledb-event-streamer requires wal_level set to 'logical'", 16)
	}

	// Log system information
	r.logger.Infof("Discovered System Information:")
	r.logger.Infof("  * PostgreSQL version %s", replicationContext.PostgresVersion().String())
	r.logger.Infof("  * TimescaleDB version %s", replicationContext.TimescaleVersion().String())
	r.logger.Infof("  * PostgreSQL System Identity %s", replicationContext.SystemId())
	r.logger.Infof("  * PostgreSQL Timeline %d", replicationContext.Timeline())
	r.logger.Infof("  * PostgreSQL Database %s", replicationContext.DatabaseName())

	// Create replication channel and internal replication handler
	replicationChannel, err := replicationchannel.NewReplicationChannel(replicationContext)
	if err != nil {
		return supporting.AdaptError(err, 18)
	}

	// Instantiate the snapshotter
	snapshotter, err := snapshotting.NewSnapshotter(32, replicationContext)
	if err != nil {
		return supporting.AdaptError(err, 19)
	}

	// Instantiate the sink type
	sink, err := r.config.SinkProvider(r.config.Config)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate sink", 22)
	}

	// Instantiate the change event emitter
	eventEmitter, err := r.config.EventEmitterProvider(r.config.Config, replicationContext, sink)
	if err != nil {
		return supporting.AdaptError(err, 14)
	}

	// Set up the system catalog (replicating the TimescaleDB internal representation)
	systemCatalog, err := intsystemcatalog.NewSystemCatalog(r.config.Config, replicationContext, snapshotter)
	if err != nil {
		return supporting.AdaptError(err, 15)
	}

	// Set up the internal transaction tracking and logical replication resolving
	transactionResolver, err := logrepresolver.NewResolver(r.config.Config, replicationContext, systemCatalog)
	if err != nil {
		return supporting.AdaptError(err, 20)
	}

	// Register event handlers
	replicationContext.RegisterReplicationEventHandler(transactionResolver)
	replicationContext.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	replicationContext.RegisterReplicationEventHandler(eventEmitter.NewEventHandler())

	// Start internal dispatching
	if err := replicationContext.StartReplicationContext(); err != nil {
		return cli.NewExitError("failed to start replication context", 18)
	}

	// Start event emitter
	if err := eventEmitter.Start(); err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to start event emitter", 24)
	}

	// Start the snapshotter
	snapshotter.StartSnapshotter()

	// Get initial list of chunks to add to publication
	initialChunkTables := systemCatalog.GetAllChunks()

	// Filter chunks by already published tables
	alreadyPublished, err := replicationContext.ReadPublishedTables()
	if err != nil {
		return supporting.AdaptError(err, 250)
	}
	initialChunkTables = supporting.Filter(initialChunkTables, func(item systemcatalog.SystemEntity) bool {
		return !supporting.ContainsWithMatcher(alreadyPublished, func(other systemcatalog.SystemEntity) bool {
			return item.CanonicalName() == other.CanonicalName()
		})
	})

	if err := replicationChannel.StartReplicationChannel(replicationContext, initialChunkTables); err != nil {
		return supporting.AdaptError(err, 16)
	}

	r.shutdownTask = func() error {
		snapshotter.StopSnapshotter()
		err1 := replicationChannel.StopReplicationChannel()
		err2 := eventEmitter.Stop()
		err3 := replicationContext.StopReplicationContext()
		return stderrors.Join(err1, err2, err3)
	}

	return nil
}

// StopReplication initiates a clean shutdown of the replication process. This
// call blocks until the shutdown process has finished.
func (r *Replicator) StopReplication() *cli.ExitError {
	if r.shutdownTask != nil {
		return supporting.AdaptError(r.shutdownTask(), 250)
	}
	return nil
}
