/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package containers

import (
	"context"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
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

	logger, err := logging.NewLogger("testcontainers")
	if err != nil {
		return nil, "", err
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
