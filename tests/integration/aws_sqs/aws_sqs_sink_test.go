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

package aws_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	inttest "github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/containers"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"testing"
	"time"
)

type AwsSqsIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestAwsSqsIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(AwsSqsIntegrationTestSuite))
}

func (asits *AwsSqsIntegrationTestSuite) Test_Aws_Sqs_Sink() {
	awsRegion := "us-east-1"
	topicPrefix := lo.RandomString(10, lo.LowerCaseLettersCharset)
	queueName := fmt.Sprintf("%s.fifo", lo.RandomString(10, lo.LowerCaseLettersCharset))

	sqsLogger, err := logging.NewLogger("Test_Aws_Sqs_Sink")
	if err != nil {
		asits.T().Error(err)
	}

	var address, endpoint string
	var container testcontainers.Container

	asits.RunTest(
		func(ctx testrunner.Context) error {
			awsConfig := aws.NewConfig().
				WithRegion(awsRegion).
				WithEndpoint(endpoint).
				WithCredentials(credentials.NewStaticCredentials("tesst", "test", "test"))

			awsSession, err := session.NewSession(awsConfig)
			if err != nil {
				return err
			}

			awsSqs := sqs.New(awsSession)
			_, err = awsSqs.CreateQueue(&sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
				Attributes: map[string]*string{
					"FifoQueue": aws.String("true"),
				},
			})
			if err != nil {
				return errors.Wrap(err, 0)
			}

			collected := make(chan bool, 1)
			envelopes := make([]inttest.Envelope, 0)
			go func() {
				for {
					msgResult, err := awsSqs.ReceiveMessage(&sqs.ReceiveMessageInput{
						AttributeNames: []*string{
							aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
						},
						MessageAttributeNames: []*string{
							aws.String(sqs.QueueAttributeNameAll),
						},
						QueueUrl:            aws.String(address),
						MaxNumberOfMessages: aws.Int64(1),
						VisibilityTimeout:   aws.Int64(60),
					})
					if err != nil {
						sqsLogger.Errorf("failed reading from sqs: %+v", err)
						collected <- true
						return
					}

					for _, message := range msgResult.Messages {
						envelope := inttest.Envelope{}
						if message.Body == nil {
							continue
						}

						if err := json.Unmarshal([]byte(*message.Body), &envelope); err != nil {
							asits.T().Error(err)
						}

						sqsLogger.Debugf("EVENT: %+v", envelope)
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
				assert.Equal(asits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
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

			container, endpoint, address, err = containers.SetupLocalStackWithSQS(awsRegion, queueName)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.AwsSQS
				config.Sink.AwsSqs = spiconfig.AwsSqsConfig{
					Queue: spiconfig.AwsSqsQueueConfig{
						Url: aws.String(address),
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
