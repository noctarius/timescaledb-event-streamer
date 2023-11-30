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

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func setupLocalStack(
	config testcontainers.CustomizeRequestOption,
) (*localstack.LocalStackContainer, error) {

	customizer := testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) {
		req.Env["PERSISTENCE"] = "1"
		req.Env["EAGER_SERVICE_LOADING"] = "1"
	})

	container, err := localstack.RunContainer(context.Background(),
		config, customizer, testcontainers.WithImage("localstack/localstack:3.0.1"))
	if err != nil {
		return nil, err
	}

	return container, nil
}

func SetupLocalStackWithSQS(
	region, queueName string,
) (testcontainers.Container, string, string, error) {

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

func SetupLocalStackWithKinesis() (testcontainers.Container, string, error) {
	customizer := testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) {
		req.Env["SERVICES"] = "kinesis"
	})

	container, err := setupLocalStack(customizer)
	if err != nil {
		return nil, "", err
	}

	host, err := container.Host(context.Background())
	if err != nil {
		return nil, "", err
	}

	port, err := container.MappedPort(context.Background(), "4566/tcp")
	if err != nil {
		return nil, "", err
	}

	return container,
		fmt.Sprintf("http://%s:%d", host, port.Int()),
		nil
}
