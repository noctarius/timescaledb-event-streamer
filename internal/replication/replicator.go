package replication

import (
	stderrors "errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/channels"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	intsystemcatalog "github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/urfave/cli"
)

type Replicator struct {
	config       *sysconfig.SystemConfig
	shutdownTask func() error
}

func NewReplicator(config *sysconfig.SystemConfig) *Replicator {
	return &Replicator{
		config: config,
	}
}

func (r *Replicator) StartReplication() *cli.ExitError {
	// Create the side channels and replication context
	replicationContext, err := context.NewReplicationContext(
		r.config.Config, r.config.PgxConfig, r.config.NamingStrategyProvider,
	)
	if err != nil {
		return cli.NewExitError("failed to initialize replication context", 17)
	}

	// Create replication channel and internal replication handler
	replicationChannel := channels.NewReplicationChannel(r.config.PgxConfig, replicationContext)

	// Read version information
	if !replicationContext.IsMinimumPostgresVersion() {
		return cli.NewExitError("timescaledb-event-streamer requires PostgreSQL 14 or later", 11)
	}

	if !replicationContext.IsMinimumTimescaleVersion() {
		return cli.NewExitError("timescaledb-event-streamer requires TimescaleDB 2.10 or later", 12)
	}

	if !replicationContext.IsLogicalReplicationEnabled() {
		return cli.NewExitError("timescaledb-event-streamer requires wal_level set to 'logical'", 16)
	}

	// Instantiate the snapshotter
	snapshotter := snapshotting.NewSnapshotter(32, replicationContext)

	// Instantiate the transaction monitor, keeping track of transaction boundaries
	transactionMonitor := transactional.NewTransactionMonitor()

	// Instantiate the change event emitter
	eventEmitter, err := r.config.EventEmitterProvider(
		r.config.Config, replicationContext, r.config.SinkProvider, transactionMonitor,
	)
	if err != nil {
		return supporting.AdaptError(err, 14)
	}

	// Set up the system catalog (replicating the TimescaleDB internal representation)
	systemCatalog, err := intsystemcatalog.NewSystemCatalog(r.config, replicationContext, snapshotter)
	if err != nil {
		return supporting.AdaptError(err, 15)
	}

	// Set up the internal transaction tracking and logical replication resolving
	transactionResolver := logicalreplicationresolver.NewResolver(r.config, replicationContext, systemCatalog)

	// Register event handlers
	replicationContext.RegisterReplicationEventHandler(transactionMonitor)
	replicationContext.RegisterReplicationEventHandler(transactionResolver)
	replicationContext.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	replicationContext.RegisterReplicationEventHandler(eventEmitter.NewEventHandler())

	// Start internal dispatching
	if err := replicationContext.StartReplicationContext(); err != nil {
		return cli.NewExitError("failed to start replication context", 18)
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
		err2 := replicationContext.StopReplicationContext()
		return stderrors.Join(err1, err2)
	}

	return nil
}

func (r *Replicator) StopReplication() *cli.ExitError {
	if r.shutdownTask != nil {
		return supporting.AdaptError(r.shutdownTask(), 250)
	}
	return nil
}
