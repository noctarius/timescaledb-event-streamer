package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func setupLocalStack(config testcontainers.CustomizeRequestOption) (*localstack.LocalStackContainer, error) {
	customizer := testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) {
		req.Env["PERSISTENCE"] = "1"
		req.Env["EAGER_SERVICE_LOADING"] = "1"
	})

	container, err := localstack.RunContainer(context.Background(), config, customizer)
	if err != nil {
		return nil, err
	}

	return container, nil
}

func SetupLocalStackWithSQS(region, queueName string) (testcontainers.Container, string, string, error) {
	customizer := testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) {
		req.Env["SQS_ENDPOINT_STRATEGY"] = "path"
		req.Env["SQS_DISABLE_CLOUDWATCH_METRICS"] = "1"
		req.Env["SERVICES"] = "sqs"
	})

	container, err := setupLocalStack(customizer)
	if err != nil {
		return nil, "", "", err
	}

	host, err := container.Host(context.Background())
	if err != nil {
		return nil, "", "", err
	}

	port, err := container.MappedPort(context.Background(), "4566/tcp")
	if err != nil {
		return nil, "", "", err
	}

	return container,
		fmt.Sprintf("http://%s:%d", host, port.Int()),
		fmt.Sprintf("http://%s:%d/queue/%s/000000000000/%s", host, port.Int(), region, queueName),
		nil
}
