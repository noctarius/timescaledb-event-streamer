package containers

import (
	"context"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

func SetupRedisContainer() (testcontainers.Container, string, error) {
	container, err := redis.StartContainer(context.Background())
	if err != nil {
		return nil, "", err
	}

	redisLogger, err := logging.NewLogger("testcontainers-redis")
	if err != nil {
		return nil, "", err
	}

	// Collect logs
	container.FollowOutput(newLogConsumer(redisLogger))
	container.StartLogProducer(context.Background())

	host, err := container.Host(context.Background())
	if err != nil {
		return nil, "", err
	}

	port, err := container.MappedPort(context.Background(), "6379/tcp")
	if err != nil {
		return nil, "", err
	}

	return container, fmt.Sprintf("%s:%d", host, port.Int()), nil
}
