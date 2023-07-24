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
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

func SetupRedisContainer() (testcontainers.Container, string, error) {
	container, err := redis.RunContainer(context.Background())
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
