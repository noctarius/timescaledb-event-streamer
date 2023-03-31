package replication

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/replication/channels"
	"github.com/noctarius/event-stream-prototype/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/snapshotting"
)

type Replicator interface {
	StartReplication() error

	StopReplication() error
}

type replicatorImpl struct {
	config       *sysconfig.SystemConfig
	shutdownTask func()
}

func NewReplicator(config *sysconfig.SystemConfig) Replicator {
	return &replicatorImpl{
		config: config,
	}
}

func (r *replicatorImpl) StartReplication() error {
	publicationName := configuring.GetOrDefault(r.config.Config, "postgresql.publication", "")
	snapshotBatchSize := configuring.GetOrDefault(r.config.Config, "postgresql.snapshot.batchsize", 1000)

	// Create the side and replication channels
	sideChannel := channels.NewSideChannel(r.config.PgxConfig, publicationName, snapshotBatchSize)
	replicationChannel := channels.NewReplicationChannel(r.config.PgxConfig, publicationName)

	// Instantiate the event dispatcher
	dispatcher := eventhandler.NewDispatcher(2000)

	// Instantiate the snapshotter
	snapshotter := snapshotting.NewSnapshotter(32, sideChannel, dispatcher)

	// Instantiate the topic name generator
	topicNameGenerator, err := r.config.NameGeneratorProvider()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	// Instantiate the schema registry keeping track of hypertable schemata
	schemaRegistry := schema.NewSchemaRegistry()

	// Instantiate the change event emitter
	eventEmitter, err := r.config.EventEmitterProvider(schemaRegistry, topicNameGenerator)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	// Set up the system catalog (replicating the Timescale internal representation)
	systemCatalog, err := systemcatalog.NewSystemCatalog(
		r.config, topicNameGenerator, dispatcher, sideChannel, schemaRegistry, snapshotter,
	)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	// Set the mapping of logical replication events to the internal catalog
	resolver := logicalreplicationresolver.NewLogicalReplicationResolver(r.config, dispatcher, systemCatalog)

	// Register event handlers
	dispatcher.RegisterReplicationEventHandler(resolver)
	dispatcher.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	dispatcher.RegisterReplicationEventHandler(eventEmitter.NewEventHandler())

	// Start dispatching events
	dispatcher.StartDispatcher()

	// Start the snapshotter
	snapshotter.StartSnapshotter()

	// Get initial list of chunks to add to publication
	initialChunkTables := systemCatalog.GetAllChunks()

	if err := replicationChannel.StartReplicationChannel(dispatcher, initialChunkTables); err != nil {
		return errors.Wrap(err, 0)
	}

	r.shutdownTask = func() {
		snapshotter.StopSnapshotter()
		replicationChannel.StopReplicationChannel()
		dispatcher.StopDispatcher()
	}

	return nil
}

func (r *replicatorImpl) StopReplication() error {
	if r.shutdownTask != nil {
		r.shutdownTask()
	}
	return nil
}
