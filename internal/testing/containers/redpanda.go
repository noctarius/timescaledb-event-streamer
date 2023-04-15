package containers

import (
	"context"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupRedPandaContainer() (testcontainers.Container, []string, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "redpandadata/redpanda:v23.1.4",
		ExposedPorts: []string{"9092:9092/tcp"},
		Cmd:          []string{"redpanda", "start"},
		WaitingFor:   wait.ForLog("Initialized cluster_id to"),
	}

	logger, err := logging.NewLogger("testcontainers")
	if err != nil {
		return nil, nil, err
	}
	redpandaLogger, err := logging.NewLogger("testcontainers-redpanda")
	if err != nil {
		return nil, nil, err
	}

	container, err := testcontainers.GenericContainer(
		context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: containerRequest,
			Started:          true,
			Logger:           logger,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	// Collect logs
	container.FollowOutput(newLogConsumer(redpandaLogger))
	container.StartLogProducer(context.Background())

	host, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	port, err := container.MappedPort(context.Background(), "9092/tcp")
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	return container, []string{fmt.Sprintf("%s:%d", host, port.Int())}, nil
}
