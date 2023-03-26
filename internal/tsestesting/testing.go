package testing

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	databaseName = "tsdb"
	postgresUser = "postgres"
	postgresPass = "postgres"
)

func SetupTimescaleContainer() (testcontainers.Container, *pgx.ConnConfig, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb@latest-pg15",
		ExposedPorts: []string{"5432/tcp"},
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
		},
	)

	if err != nil {
		return nil, nil, err
	}

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

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		postgresUser, postgresPass, host, port.Port(), databaseName)

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	conn.Exec(context.Background(), "CREATE EXTENSION timescaledb")

	return container, config, nil
}
