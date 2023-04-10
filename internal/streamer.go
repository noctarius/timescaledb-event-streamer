package internal

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/pkg/errors"
)

const publicationName = "pg_ts_streamer"

type Streamer struct {
	replicator replication.Replicator
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

	pgPublication := configuring.GetOrDefault(config.Config, "postgresql.publication.name", "")
	if pgPublication == "" {
		config.PostgreSQL.Publication.Name = publicationName
	}

	if config.Topic.Prefix == "" {
		config.Topic.Prefix = supporting.RandomTextString(20)
	}

	return &Streamer{
		replicator: replication.NewReplicator(config),
	}, nil, 0
}

func (s *Streamer) Start() error {
	if err := s.replicator.StartReplication(); err != nil {
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
