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

package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/tests/integration"
	inttest "github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/containers"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"testing"
	"time"
)

type KafkaIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestKafkaIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(KafkaIntegrationTestSuite))
}

func (kits *KafkaIntegrationTestSuite) Test_Kafka_Sink() {
	topicPrefix := supporting.RandomTextString(10)

	var container testcontainers.Container

	kits.RunTest(
		func(ctx testrunner.Context) error {
			topicName := fmt.Sprintf(
				"%s.%s.%s", topicPrefix,
				testrunner.GetAttribute[string](ctx, "schemaName"),
				testrunner.GetAttribute[string](ctx, "tableName"),
			)

			groupName := supporting.RandomTextString(10)

			config := sarama.NewConfig()
			client, err := sarama.NewConsumerGroup(testrunner.GetAttribute[[]string](ctx, "brokers"), groupName, config)
			if err != nil {
				return err
			}

			consumer, ready := integration.NewKafkaConsumer(kits.T())
			go func() {
				if err := client.Consume(context.Background(), []string{topicName}, consumer); err != nil {
					kits.T().Error(err)
				}
			}()

			<-ready

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			<-consumer.Collected()

			for i, envelope := range consumer.Envelopes() {
				assert.Equal(kits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
			}
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			sn, tn, err := setupContext.CreateHypertable("ts", time.Hour*24,
				inttest.NewColumn("ts", "timestamptz", false, false, nil),
				inttest.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(setupContext, "schemaName", sn)
			testrunner.Attribute(setupContext, "tableName", tn)

			kC, brokers, err := containers.SetupKafkaContainer()
			if err != nil {
				return errors.Wrap(err, 0)
			}
			container = kC
			testrunner.Attribute(setupContext, "brokers", brokers)

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.Kafka
				config.Sink.Kafka = spiconfig.KafkaConfig{
					Brokers:    brokers,
					Idempotent: false,
				}
			})
			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			if container != nil {
				container.Terminate(context.Background())
			}
			return nil
		}),
	)
}
