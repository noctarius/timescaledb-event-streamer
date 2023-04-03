package containers

import (
	"context"
	"fmt"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var redpandaLogger = logging.NewLogger("testcontainers-redpanda")

func SetupRedPandaContainer() (testcontainers.Container, []string, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "redpandadata/redpanda:v23.1.4",
		ExposedPorts: []string{"9092:9092/tcp"},
		Cmd:          []string{"redpanda", "start"},
		WaitingFor:   wait.ForLog("Initialized cluster_id to"),
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
