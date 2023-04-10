package containers

import (
	"context"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const natsProtocol = "nats"

func SetupNatsContainer() (testcontainers.Container, string, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "nats:2.9-alpine",
		ExposedPorts: []string{"4222/tcp", "8222/tcp"},
		Cmd:          []string{"--js"},
		WaitingFor:   wait.NewLogStrategy("Server is ready"),
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
		return nil, "nil", err
	}

	host, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		return nil, "", err
	}

	natsPort, err := container.MappedPort(context.Background(), "4222/tcp")
	if err != nil {
		container.Terminate(context.Background())
		return nil, "", err
	}

	return container, fmt.Sprintf("%s://%s:%d", natsProtocol, host, natsPort.Int()), nil
}
