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

package aws_kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/containers"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"testing"
	"time"
)

type AwsKinesisIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestAwsKinesisIntegrationTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(AwsKinesisIntegrationTestSuite))
}

func (akits *AwsKinesisIntegrationTestSuite) Test_Aws_Kinesis_Sink() {
	awsRegion := "us-east-1"
	topicPrefix := lo.RandomString(10, lo.LowerCaseLettersCharset)
	streamName := lo.RandomString(10, lo.LowerCaseLettersCharset)

	kinesisLogger, err := logging.NewLogger("Test_Aws_Kinesis_Sink")
	if err != nil {
		akits.T().Error(err)
	}

	var endpoint string
	var container testcontainers.Container

	akits.RunTest(
		func(ctx testrunner.Context) error {
			awsConfig := aws.NewConfig().
				WithRegion(awsRegion).
				WithEndpoint(endpoint).
				WithCredentials(credentials.NewStaticCredentials("test", "test", "test"))

			awsSession, err := session.NewSession(awsConfig)
			if err != nil {
				return err
			}

			awsKinesis := kinesis.New(awsSession)

			collected := make(chan bool, 1)
			envelopes := make([]testsupport.Envelope, 0)
			go func() {
				shardsResult, err := awsKinesis.ListShards(&kinesis.ListShardsInput{
					StreamName: aws.String(streamName),
				})
				if err != nil {
					akits.T().Error(err)
					collected <- true
					return
				}

				var lastSequenceNumber *string
				for {
					iteratorType := "TRIM_HORIZON"
					if lastSequenceNumber != nil {
						iteratorType = "AFTER_SEQUENCE_NUMBER"
					}

					iteratorResult, err := awsKinesis.GetShardIterator(&kinesis.GetShardIteratorInput{
						ShardId:                shardsResult.Shards[0].ShardId,
						ShardIteratorType:      aws.String(iteratorType),
						StreamName:             aws.String(streamName),
						StartingSequenceNumber: lastSequenceNumber,
					})
					if err != nil {
						kinesisLogger.Errorf("failed reading from kinesis: %+v", err)
						collected <- true
						return
					}

					msgResult, err := awsKinesis.GetRecords(&kinesis.GetRecordsInput{
						ShardIterator: iteratorResult.ShardIterator,
					})
					if err != nil {
						kinesisLogger.Errorf("failed reading from kinesis: %+v", err)
						collected <- true
						return
					}

					for _, message := range msgResult.Records {
						lastSequenceNumber = message.SequenceNumber

						envelope := testsupport.Envelope{}
						if message.Data == nil {
							continue
						}

						if err := json.Unmarshal(message.Data, &envelope); err != nil {
							akits.T().Error(err)
						}

						kinesisLogger.Debugf("EVENT: %+v", envelope)
						envelopes = append(envelopes, envelope)
						if len(envelopes) >= 10 {
							collected <- true
							return
						}
					}
				}
			}()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			for i, envelope := range envelopes {
				assert.Equal(akits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
			}
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			sn, tn, err := setupContext.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(setupContext, "schemaName", sn)
			testrunner.Attribute(setupContext, "tableName", tn)

			container, endpoint, err = containers.SetupLocalStackWithKinesis()
			if err != nil {
				return errors.Wrap(err, 0)
			}

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.AwsKinesis
				config.Sink.AwsKinesis = spiconfig.AwsKinesisConfig{
					Stream: spiconfig.AwsKinesisStreamConfig{
						Name:       aws.String(streamName),
						Create:     aws.Bool(true),
						ShardCount: aws.Int64(1),
					},
					Aws: spiconfig.AwsConnectionConfig{
						Region:          aws.String(awsRegion),
						AccessKeyId:     "test",
						SecretAccessKey: "test",
						SessionToken:    "test",
						Endpoint:        endpoint,
					},
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
