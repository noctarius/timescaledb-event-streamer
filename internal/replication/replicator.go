package replication

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/snapshotting"
)

type Replicator interface {
	StartReplication(schemaRegistry *schema.Registry,
		topicNameGenerator *topic.NameGenerator, eventEmitter *sink.EventEmitter) error
	StopReplication() error
}

type replicatorImpl struct {
	sideChannel        *sideChannel
	replicationChannel *replicationChannel
	publicationName    string
	config             *configuring.Config
	connConfig         *pgx.ConnConfig
	shutdownTask       func()
}

func NewReplicator(config *configuring.Config, connConfig *pgx.ConnConfig) Replicator {
	publicationName := configuring.GetOrDefault(config, "postgresql.publication", "")
	snapshotBatchSize := configuring.GetOrDefault(config, "postgresql.snapshot.batchsize", 1000)
	return &replicatorImpl{
		config:             config,
		connConfig:         connConfig,
		publicationName:    publicationName,
		sideChannel:        newSideChannel(connConfig, publicationName, snapshotBatchSize),
		replicationChannel: newReplicationChannel(connConfig, publicationName),
	}
}

func (r *replicatorImpl) StartReplication(schemaRegistry *schema.Registry,
	topicNameGenerator *topic.NameGenerator, eventEmitter *sink.EventEmitter) error {

	// Instantiate the event dispatcher
	dispatcher := eventhandler.NewDispatcher(2000)

	// Instantiate the snapshotter
	snapshotter := snapshotting.NewSnapshotter(32, r.sideChannel, dispatcher)

	// Set up the system catalog (replicating the Timescale internal representation)
	systemCatalog, err := systemcatalog.NewSystemCatalog(r.connConfig.Database, r.publicationName, r.config,
		schemaRegistry, topicNameGenerator, dispatcher, r.sideChannel, snapshotter)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	// Set the mapping of logical replication events to the internal catalog
	resolver := logicalreplicationresolver.NewLogicalReplicationResolver(r.config, dispatcher, systemCatalog)

	// Register event handlers
	dispatcher.RegisterReplicationEventHandler(resolver)
	dispatcher.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	dispatcher.RegisterReplicationEventHandler(eventEmitter.NewEventHandler(true))

	// Start dispatching events
	dispatcher.StartDispatcher()

	// Start the snapshotter
	snapshotter.StartSnapshotter()

	// Get initial list of chunks to add to publication
	initialChunkTables := systemCatalog.GetAllChunks()

	if err := r.replicationChannel.startReplicationChannel(dispatcher, initialChunkTables); err != nil {
		return errors.Wrap(err, 0)
	}

	r.shutdownTask = func() {
		snapshotter.StopSnapshotter()
		dispatcher.StopDispatcher()
	}

	return nil
}

func (r *replicatorImpl) StopReplication() error {
	r.replicationChannel.stopReplicationChannel()
	if r.shutdownTask != nil {
		r.shutdownTask()
	}
	return nil
}
