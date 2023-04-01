package testing

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"strings"
	"time"
)

var logger = logging.NewLogger("Test Support")

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

const (
	PublicationName = "testpub"
)

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
		Cmd:          []string{"postgres", "-c", "fsync=off", "-c", "wal_level=logical"},
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
	logger.Println("Drop timescaledb extension")
	if err := exec("DROP EXTENSION IF EXISTS timescaledb"); err != nil {
		return nil, nil, err
	}

	// Create default user role
	logger.Println("Create default user login")
	if err := exec(
		fmt.Sprintf("CREATE ROLE %s LOGIN ENCRYPTED PASSWORD '%s'", tsdbUser, tsdbPass),
	); err != nil {
		return nil, nil, err
	}
	logger.Println("Grant permissions to default user")
	if err := exec(
		fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", databaseName, tsdbUser),
	); err != nil {
		return nil, nil, err
	}
	logger.Printf("Create %s schema", databaseSchema)
	if err := exec(
		fmt.Sprintf("CREATE SCHEMA %s", databaseSchema),
	); err != nil {
		return nil, nil, err
	}
	logger.Println("Grant schema permissions to default user")
	if err := exec(
		fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", databaseSchema, tsdbUser),
	); err != nil {
		return nil, nil, err
	}
	logger.Println("Set default schema for default user")
	if err := exec(
		fmt.Sprintf("ALTER ROLE %s SET search_path TO %s, public", tsdbUser, databaseSchema),
	); err != nil {
		return nil, nil, err
	}

	// Create replication user and adjust permissions
	logger.Println("Create replication user login")
	if err := exec(
		fmt.Sprintf("CREATE ROLE %s LOGIN REPLICATION ENCRYPTED PASSWORD '%s'", replUser, replPass),
	); err != nil {
		return nil, nil, err
	}
	logger.Println("Grant user permissions for replication user")
	if err := exec(
		fmt.Sprintf("GRANT %s TO %s", tsdbUser, replUser),
	); err != nil {
		return nil, nil, err
	}
	logger.Println("Create timescaledb extension")
	if err := exec("CREATE EXTENSION IF NOT EXISTS timescaledb"); err != nil {
		return nil, nil, err
	}
	logger.Println("Drop existing publication")

	// Create initial publication function
	if err := exec(
		fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", PublicationName),
	); err != nil {
		return nil, nil, err
	}
	logger.Println("Create publication initiator function")
	if err := exec(initialPublicationFunction); err != nil {
		return nil, nil, err
	}

	// Close database setup connection when done
	conn.Close(context.Background())

	return container, &ConfigProvider{host, port.Int()}, nil
}

func CreateHypertable(pool *pgxpool.Pool, timeDimension string,
	chunkSize time.Duration, columns ...model.Column) (string, string, error) {

	tableName := randomTableName()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	columnDefinitions := make([]string, len(columns))
	for i, column := range columns {
		columnDefinitions[i] = toDefinition(column)
	}

	query := fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (%s)", databaseSchema,
		tableName, strings.Join(columnDefinitions, ", "))

	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	query = fmt.Sprintf(
		"SELECT create_hypertable('%s.%s', '%s', chunk_time_interval := interval '%d seconds')",
		databaseSchema, tableName, timeDimension, int64(chunkSize.Seconds()),
	)
	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	tx.Commit(context.Background())
	return databaseSchema, tableName, nil
}

func randomTableName() string {
	return supporting.RandomTextString(20)
}

func toDefinition(column model.Column) string {
	builder := strings.Builder{}
	builder.WriteString(column.Name())
	builder.WriteString(" ")
	builder.WriteString(column.TypeName())
	if column.Nullable() {
		builder.WriteString(" NULL")
	}
	if column.DefaultValue() != nil {
		builder.WriteString(fmt.Sprintf(" DEFAULT '%s'", *column.DefaultValue()))
	}
	if column.PrimaryKey() {
		builder.WriteString(" PRIMARY KEY")
	}
	return builder.String()
}
