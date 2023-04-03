package containers

import (
	"context"
	"fmt"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"math"
)

const starterScript = "/usr/sbin/testcontainers_start.sh"

var kafkaLogger = logging.NewLogger("testcontainers-kafka")

func SetupKafkaContainer() (testcontainers.Container, []string, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.3.3",
		ExposedPorts: []string{"9093/tcp"},
		Env: map[string]string{
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_BROKER_ID":                                "1",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES":              fmt.Sprintf("%d", math.MaxInt64),
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
		},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount("/var/run/docker.sock", "/var/run/docker.sock"),
		),
		Entrypoint: []string{"sh"},
		Cmd:        []string{"-c", "while [ ! -f " + starterScript + " ]; do sleep 0.1; done; bash " + starterScript},
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
	container.FollowOutput(newLogConsumer(kafkaLogger))
	container.StartLogProducer(context.Background())

	host, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	port, err := container.MappedPort(context.Background(), "9093/tcp")
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	script := fmt.Sprintf(`#!/bin/bash
source /etc/confluent/docker/bash-config
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:%d,BROKER://%s:9092
echo Starting Kafka KRaft mode
sed -i '/KAFKA_ZOOKEEPER_CONNECT/d' /etc/confluent/docker/configure
echo 'kafka-storage format --ignore-formatted -t "$(kafka-storage random-uuid)" -c /etc/kafka/kafka.properties' >> /etc/confluent/docker/configure
echo '' > /etc/confluent/docker/ensure
/etc/confluent/docker/configure
/etc/confluent/docker/launch`,
		host, port.Int(), host)

	if err := container.CopyToContainer(context.Background(), []byte(script), starterScript, 700); err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	if err := wait.ForLog("Kafka Server started").WaitUntilReady(context.Background(), container); err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	return container, []string{fmt.Sprintf("%s:%d", host, port.Int())}, nil
}
