package replication

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog"
)

type Replicator interface {
	StartReplication(schemaRegistry *schema.Registry,
		topicNameGenerator *topic.NameGenerator, eventEmitter *sink.EventEmitter) error
	StopReplication() error
}

type replicatorImpl struct {
	sideChannel        *sideChannel
	replicationChannel *replicationChannel
	config             *configuration.Config
	connConfig         *pgx.ConnConfig
}

func NewReplicator(config *configuration.Config, connConfig *pgx.ConnConfig) Replicator {
	return &replicatorImpl{
		config:             config,
		connConfig:         connConfig,
		sideChannel:        newSideChannel(connConfig),
		replicationChannel: newReplicationChannel(connConfig, config.PostgreSQL.Publication),
	}
}

func (r *replicatorImpl) StartReplication(schemaRegistry *schema.Registry,
	topicNameGenerator *topic.NameGenerator, eventEmitter *sink.EventEmitter) error {

	// Instantiate the event dispatcher
	dispatcher := eventhandler.NewDispatcher(1000)

	// Set up the system catalog (replicating the Timescale internal representation)
	systemCatalog, err := systemcatalog.NewSystemCatalog(r.connConfig.Database, r.config,
		schemaRegistry, topicNameGenerator, dispatcher, r.sideChannel)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	// Set the mapping of logical replication events to the internal catalog
	resolver := logicalreplicationresolver.NewLogicalReplicationResolver(dispatcher, systemCatalog)

	// Register event handlers
	dispatcher.RegisterReplicationEventHandler(resolver)
	dispatcher.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	dispatcher.RegisterReplicationEventHandler(eventEmitter.NewEventHandler(true))

	// Start dispatching events
	dispatcher.StartDispatcher()

	// Get initial list of chunks to add to publication
	initialChunkTables := systemCatalog.GetAllChunks()

	if err := r.replicationChannel.startReplicationChannel(dispatcher, initialChunkTables); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (r *replicatorImpl) StopReplication() error {
	r.replicationChannel.stopReplicationChannel()
	return nil
}
