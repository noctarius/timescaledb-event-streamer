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

package awssqs

import (
	"crypto/sha256"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-errors/errors"
	config "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sink.RegisterSink(config.AwsSQS, newAwsSqsSink)
}

type awsSqsSink struct {
	queueUrl *string
	awsSqs   *sqs.SQS
	encoder  *encoding.JsonEncoder
}

func newAwsSqsSink(
	c *config.Config,
) (sink.Sink, error) {

	queueUrl := config.GetOrDefault[*string](c, config.PropertySqsQueueUrl, nil)
	if queueUrl == nil {
		return nil, errors.Errorf("AWS SQS sink needs the queue url to be configured")
	}

	awsRegion := config.GetOrDefault[*string](c, config.PropertySqsAwsRegion, nil)
	endpoint := config.GetOrDefault(c, config.PropertySqsAwsEndpoint, "")
	accessKeyId := config.GetOrDefault[*string](c, config.PropertySqsAwsAccessKeyId, nil)
	secretAccessKey := config.GetOrDefault[*string](c, config.PropertySqsAwsSecretAccessKey, nil)
	sessionToken := config.GetOrDefault[*string](c, config.PropertySqsAwsSessionToken, nil)

	awsConfig := aws.NewConfig().WithEndpoint(endpoint)
	if accessKeyId != nil && secretAccessKey != nil && sessionToken != nil {
		awsConfig = awsConfig.WithCredentials(
			credentials.NewStaticCredentials(*accessKeyId, *secretAccessKey, *sessionToken),
		)
	}

	if awsRegion != nil {
		awsConfig = awsConfig.WithRegion(*awsRegion)
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	return &awsSqsSink{
		queueUrl: queueUrl,
		awsSqs:   sqs.New(awsSession),
		encoder:  encoding.NewJsonEncoderWithConfig(c),
	}, nil
}

func (a *awsSqsSink) Start() error {
	return nil
}

func (a *awsSqsSink) Stop() error {
	return nil
}

func (a *awsSqsSink) Emit(
	_ sink.Context, _ time.Time, topicName string, _, envelope schema.Struct,
) error {

	envelopeData, err := a.encoder.Marshal(envelope)
	if err != nil {
		return err
	}

	payload := envelope[schema.FieldNamePayload].(schema.Struct)
	source := payload[schema.FieldNameSource].(schema.Struct)
	lsn := source[schema.FieldNameLSN].(string)
	txId, present := source[schema.FieldNameTxId]

	var msgDeduplicationIdContent string
	if present {
		msgDeduplicationIdContent = fmt.Sprintf("%s-%d-%s", lsn, *(txId.(*uint32)), envelopeData)
	} else {
		msgDeduplicationIdContent = fmt.Sprintf("%s-%s", lsn, envelopeData)
	}

	hash := sha256.New()
	hash.Write([]byte(msgDeduplicationIdContent))
	msgDeduplicationId := fmt.Sprintf("%X", hash.Sum(nil))

	_, err = a.awsSqs.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:           aws.Int64(0),
		MessageBody:            aws.String(string(envelopeData)),
		MessageGroupId:         aws.String(topicName),
		MessageDeduplicationId: aws.String(msgDeduplicationId),
		QueueUrl:               a.queueUrl,
	})
	return err
}
