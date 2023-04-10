package internal

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/urfave/cli"
)

const publicationName = "pg_ts_streamer"

type Streamer struct {
	replicator replication.Replicator
}

func NewStreamer(config *sysconfig.SystemConfig) (*Streamer, *cli.ExitError) {
	if config.PgxConfig == nil {
		connection := configuring.GetOrDefault(config.Config, "postgresql.connection", "host=localhost user=repl_user")
		connConfig, err := pgx.ParseConfig(connection)
		if err != nil {
			return nil, cli.NewExitError(
				fmt.Sprintf("PostgreSQL connection string failed to parse: %s", err.Error()), 6)
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
	}, nil
}

func (s *Streamer) Start() *cli.ExitError {
	return s.replicator.StartReplication()
}

func (s *Streamer) Stop() *cli.ExitError {
	return s.replicator.StopReplication()
}
