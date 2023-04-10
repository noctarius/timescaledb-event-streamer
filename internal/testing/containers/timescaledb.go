package containers

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"time"
)

const (
	databaseName   = "tsdb"
	databaseSchema = "tsdb"
	postgresUser   = "postgres"
	postgresPass   = "postgres"
	tsdbUser       = "tsdb"
	tsdbPass       = "tsdb"
	replUser       = "repl_user"
	replPass       = "repl_user"
)

var timescaledbLogger = logging.NewLogger("testcontainers-timescaledb")

const initialPublicationFunction = `
CREATE OR REPLACE FUNCTION create_timescaledb_catalog_publication(publication_name text, replication_user text)
    RETURNS bool
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
DECLARE
    found bool;
    owner oid;
BEGIN
    SELECT true, pubowner
    FROM pg_catalog.pg_publication
    WHERE pubname = publication_name
    INTO found, owner;

    IF found THEN
        SELECT true
        FROM pg_catalog.pg_publication_tables
        WHERE pubname = publication_name
          AND schemaname = '_timescaledb_catalog'
          AND tablename = 'hypertable'
        INTO found;

        IF NOT found THEN
            RAISE EXCEPTION 'Publication % already exists but is missing _timescaledb_catalog.hypertable', publication_name;
        END IF;

        SELECT true
        FROM pg_catalog.pg_publication_tables
        WHERE pubname = publication_name
          AND schemaname = '_timescaledb_catalog'
          AND tablename = 'chunk'
        INTO found;

        IF NOT found THEN
            RAISE EXCEPTION 'Publication % already exists but is missing _timescaledb_catalog.chunk', publication_name;
        END IF;

        SELECT true FROM (
            SELECT session_user as uid
        ) s
        WHERE s.uid = owner
        INTO found;

        IF NOT found THEN
            RAISE EXCEPTION 'Publication % already exists but is not owned by the session user', publication_name;
        END IF;

        RETURN true;
    END IF;

    EXECUTE format('CREATE PUBLICATION %I FOR TABLE _timescaledb_catalog.chunk, _timescaledb_catalog.hypertable', publication_name);
    EXECUTE format('ALTER PUBLICATION %I OWNER TO %s', publication_name, replication_user);
    RETURN true;
END;
$$
`

type ConfigProvider struct {
	host string
	port int
}

func (c *ConfigProvider) ReplicationConnConfig() (*pgx.ConnConfig, error) {
	return pgx.ParseConfig(
		fmt.Sprintf("postgres://%s:%s@%s:%d/%s", replUser, replPass, c.host, c.port, databaseName),
	)
}

func (c *ConfigProvider) UserConnConfig() (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(
		fmt.Sprintf("postgres://%s:%s@%s:%d/%s", tsdbUser, tsdbPass, c.host, c.port, databaseName),
	)
}

func SetupTimescaleContainer() (testcontainers.Container, *ConfigProvider, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg15",
		ExposedPorts: []string{"5432/tcp"},
		Cmd:          []string{"-c", "fsync=off", "-c", "wal_level=logical"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_DB":       databaseName,
			"POSTGRES_PASSWORD": postgresUser,
			"POSTGRES_USER":     postgresPass,
		},
	}

	container, err := testcontainers.GenericContainer(
		context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: containerRequest,
			Started:          true,
			Logger:           logging.NewLogger("testcontainers"),
		},
	)
	if err != nil {
		return nil, nil, err
	}

	// Collect logs
	//container.FollowOutput(&logConsumer{})
	//container.StartLogProducer(context.Background())

	host, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	port, err := container.MappedPort(context.Background(), "5432/tcp")
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		postgresUser, postgresPass, host, port.Int(), databaseName)

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	var conn *pgx.Conn
	for i := 0; ; i++ {
		conn, err = pgx.ConnectConfig(context.Background(), config)
		if err != nil {
			if i == 9 {
				_ = container.Terminate(context.Background())
				return nil, nil, err
			} else {
				time.Sleep(time.Second)
			}
		} else {
			break
		}
	}

	exec := func(query string) error {
		if _, err := conn.Exec(context.Background(), query); err != nil {
			conn.Close(context.Background())
			container.Terminate(context.Background())
			return err
		}
		return nil
	}

	// Disable timescale plugin
	timescaledbLogger.Verbosef("Drop timescaledb extension")
	if err := exec("DROP EXTENSION IF EXISTS timescaledb"); err != nil {
		return nil, nil, err
	}

	// Create default user role
	timescaledbLogger.Verbosef("Create default user login")
	if err := exec(
		fmt.Sprintf("CREATE ROLE %s LOGIN ENCRYPTED PASSWORD '%s'", tsdbUser, tsdbPass),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Grant permissions to default user")
	if err := exec(
		fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", databaseName, tsdbUser),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Create %s schema", databaseSchema)
	if err := exec(
		fmt.Sprintf("CREATE SCHEMA %s", databaseSchema),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Grant schema permissions to default user")
	if err := exec(
		fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", databaseSchema, tsdbUser),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Set default schema for default user")
	if err := exec(
		fmt.Sprintf("ALTER ROLE %s SET search_path TO %s, public", tsdbUser, databaseSchema),
	); err != nil {
		return nil, nil, err
	}

	// Create replication user and adjust permissions
	timescaledbLogger.Verbosef("Create replication user login")
	if err := exec(
		fmt.Sprintf("CREATE ROLE %s LOGIN REPLICATION ENCRYPTED PASSWORD '%s'", replUser, replPass),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Grant user permissions for replication user")
	if err := exec(
		fmt.Sprintf("GRANT %s TO %s", tsdbUser, replUser),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Create timescaledb extension")
	if err := exec("CREATE EXTENSION IF NOT EXISTS timescaledb"); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Drop existing publication")

	// Create initial publication function
	timescaledbLogger.Verbosef("Create publication initiator function")
	if err := exec(initialPublicationFunction); err != nil {
		return nil, nil, err
	}

	// Close database setup connection when done
	conn.Close(context.Background())

	return container, &ConfigProvider{host, port.Int()}, nil
}
