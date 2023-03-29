package internal

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/replication"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"github.com/pkg/errors"
)

const publicationName = "pg_ts_streamer"

type Streamer struct {
	replicator         replication.Replicator
	schemaRegistry     *schema.Registry
	topicNameGenerator *topic.NameGenerator
	eventEmitter       *sink.EventEmitter
}

func NewStreamer(config *sysconfig.SystemConfig) (*Streamer, error, int) {
	if config.PgxConfig == nil {
		connection := configuring.GetOrDefault(config.Config, "postgresql.connection", "host=localhost user=repl_user")
		connConfig, err := pgx.ParseConfig(connection)
		if err != nil {
			return nil, errors.Wrap(err, "PostgreSQL connection string failed to parse"), 6
		}

		pgPassword := configuring.GetOrDefault(config.Config, "postgresql.password", "")
		if pgPassword != "" {
			connConfig.Password = pgPassword
		}

		config.PgxConfig = connConfig
	}

	pgPublication := configuring.GetOrDefault(config.Config, "postgresql.publication", "")
	if pgPublication == "" {
		config.PostgreSQL.Publication = publicationName
	}

	if config.Topic.Prefix == "" {
		config.Topic.Prefix = supporting.RandomTextString(20)
	}

	replicator := replication.NewReplicator(config.Config, config.PgxConfig)
	schemaRegistry := schema.NewSchemaRegistry()

	topicNameGenerator, err := config.NameGeneratorProvider()
	if err != nil {
		return nil, err, 7
	}

	eventEmitter, err := config.EventEmitterProvider(schemaRegistry, topicNameGenerator)
	if err != nil {
		return nil, err, 8
	}

	return &Streamer{
		replicator:         replicator,
		schemaRegistry:     schemaRegistry,
		topicNameGenerator: topicNameGenerator,
		eventEmitter:       eventEmitter,
	}, nil, 0
}

func (s *Streamer) Start() error {
	if err := s.replicator.StartReplication(s.schemaRegistry, s.topicNameGenerator, s.eventEmitter); err != nil {
		return err
	}
	return nil
}

func (s *Streamer) Stop() error {
	if err := s.replicator.StopReplication(); err != nil {
		return err
	}
	return nil
}
