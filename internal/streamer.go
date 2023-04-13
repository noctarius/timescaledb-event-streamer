package internal

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/urfave/cli"

	// Register built-in naming strategies
	_ "github.com/noctarius/timescaledb-event-streamer/internal/event/topic"

	// Register built-in sinks
	_ "github.com/noctarius/timescaledb-event-streamer/internal/event/sink/kafka"
	_ "github.com/noctarius/timescaledb-event-streamer/internal/event/sink/nats"
	_ "github.com/noctarius/timescaledb-event-streamer/internal/event/sink/redis"
	_ "github.com/noctarius/timescaledb-event-streamer/internal/event/sink/stdout"
)

const publicationName = "pg_ts_streamer"

type Streamer struct {
	replicator replication.Replicator
}

func NewStreamer(config *sysconfig.SystemConfig) (*Streamer, *cli.ExitError) {
	if config.PgxConfig == nil {
		connection := spiconfig.GetOrDefault(config.Config, "postgresql.connection", "host=localhost user=repl_user")
		connConfig, err := pgx.ParseConfig(connection)
		if err != nil {
			return nil, cli.NewExitError(
				fmt.Sprintf("PostgreSQL connection string failed to parse: %s", err.Error()), 6)
		}

		pgPassword := spiconfig.GetOrDefault(config.Config, "postgresql.password", "")
		if pgPassword != "" {
			connConfig.Password = pgPassword
		}

		config.PgxConfig = connConfig
	}

	pgPublication := spiconfig.GetOrDefault(config.Config, "postgresql.publication.name", "")
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
