package internal

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/replication"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/pkg/errors"
)

const publicationName = "pg_ts_streamer"

type Streamer struct {
	shutdownDone       chan bool
	replicator         replication.Replicator
	schemaRegistry     *schema.Registry
	topicNameGenerator *topic.NameGenerator
	eventEmitter       *sink.EventEmitter
}

func NewStreamer(config *configuration.Config) (*Streamer, error, int) {
	if config.PostgreSQL.PgxConfig == nil {
		connection := configuration.GetOrDefault(config, "postgresql.connection", "host=localhost user=repl_user")
		connConfig, err := pgx.ParseConfig(connection)
		if err != nil {
			return nil, errors.Wrap(err, "PostgreSQL connection string failed to parse"), 6
		}

		pgPassword := configuration.GetOrDefault(config, "postgresql.password", "")
		if pgPassword != "" {
			connConfig.Password = pgPassword
		}

		config.PostgreSQL.PgxConfig = connConfig
	}

	pgPublication := configuration.GetOrDefault(config, "postgresql.publication", "")
	if pgPublication == "" {
		config.PostgreSQL.Publication = publicationName
	}

	replicator := replication.NewReplicator(config, config.PostgreSQL.PgxConfig)
	schemaRegistry := schema.NewSchemaRegistry()

	topicNameGenerator, err := newNameGenerator(config)
	if err != nil {
		return nil, err, 7
	}

	eventEmitter, err := newEventEmitter(config, schemaRegistry, topicNameGenerator)
	if err != nil {
		return nil, err, 8
	}

	return &Streamer{
		replicator:         replicator,
		schemaRegistry:     schemaRegistry,
		topicNameGenerator: topicNameGenerator,
		eventEmitter:       eventEmitter,
		shutdownDone:       make(chan bool, 1),
	}, nil, 0
}

func (s *Streamer) Start() error {
	if err := s.replicator.StartReplication(s.schemaRegistry, s.topicNameGenerator, s.eventEmitter); err != nil {
		return err
	}
	s.shutdownDone <- true
	return nil
}

func (s *Streamer) Stop() error {
	if err := s.replicator.StopReplication(); err != nil {
		return err
	}
	<-s.shutdownDone
	return nil
}
