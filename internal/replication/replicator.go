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
}

func NewReplicator(config *configuring.Config, connConfig *pgx.ConnConfig) Replicator {
	publicationName := configuring.GetOrDefault(config, "postgresql.publication", "")
	return &replicatorImpl{
		config:             config,
		connConfig:         connConfig,
		publicationName:    publicationName,
		sideChannel:        newSideChannel(connConfig),
		replicationChannel: newReplicationChannel(connConfig, publicationName),
	}
}

func (r *replicatorImpl) StartReplication(schemaRegistry *schema.Registry,
	topicNameGenerator *topic.NameGenerator, eventEmitter *sink.EventEmitter) error {

	// Instantiate the event dispatcher
	dispatcher := eventhandler.NewDispatcher(2000)

	// Set up the system catalog (replicating the Timescale internal representation)
	systemCatalog, err := systemcatalog.NewSystemCatalog(r.connConfig.Database, r.publicationName, r.config,
		schemaRegistry, topicNameGenerator, dispatcher, r.sideChannel)
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
