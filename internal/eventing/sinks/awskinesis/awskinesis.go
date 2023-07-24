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

package awskinesis

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/go-errors/errors"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"log"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.AwsKinesis, newAwsKinesisSink)
}

type awsKinesisSink struct {
	streamName *string
	awsKinesis *kinesis.Kinesis
}

func newAwsKinesisSink(
	config *spiconfig.Config,
) (sink.Sink, error) {

	streamName := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisStreamName, nil)
	if streamName == nil {
		return nil, errors.Errorf("AWS Kinesis sink needs the stream name to be configured")
	}

	shardCount := spiconfig.GetOrDefault[*int64](config, spiconfig.PropertyKinesisStreamShardCount, nil)
	streamMode := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisStreamMode, nil)
	streamCreate := spiconfig.GetOrDefault(config, spiconfig.PropertyKinesisStreamCreate, true)

	awsRegion := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisRegion, nil)
	endpoint := spiconfig.GetOrDefault(config, spiconfig.PropertyKinesisAwsEndpoint, "")
	accessKeyId := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisAwsAccessKeyId, nil)
	secretAccessKey := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisAwsSecretAccessKey, nil)
	sessionToken := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisAwsSessionToken, nil)

	awsConfig := aws.NewConfig().WithEndpoint(endpoint)
	if accessKeyId != nil && secretAccessKey != nil && sessionToken != nil {
		awsConfig = awsConfig.WithCredentials(
			credentials.NewStaticCredentials(*accessKeyId, *secretAccessKey, *sessionToken),
		)
	}

	if awsRegion != nil {
		awsConfig = awsConfig.WithRegion(*awsRegion)
	}

	var streamModeDetails *kinesis.StreamModeDetails
	if streamMode != nil {
		streamModeDetails = &kinesis.StreamModeDetails{
			StreamMode: streamName,
		}
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	awsKinesis := kinesis.New(awsSession)
	_, err = awsKinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: streamName,
	})
	if err != nil {
		if _, ok := err.(*kinesis.ResourceNotFoundException); !ok {
			return nil, err
		}

		// Resource (stream) doesn't exist yet, we may want to create it automatically
		if !streamCreate {
			return nil, err
		}

		// Create the stream in AWS Kinesis
		if _, err = awsKinesis.CreateStream(&kinesis.CreateStreamInput{
			ShardCount:        shardCount,
			StreamModeDetails: streamModeDetails,
			StreamName:        streamName,
		}); err != nil {
			return nil, err
		}

		if err := awsKinesis.WaitUntilStreamExists(&kinesis.DescribeStreamInput{
			StreamName: streamName,
		}); err != nil {
			log.Panic(err)
		}
	}

	return &awsKinesisSink{
		streamName: streamName,
		awsKinesis: awsKinesis,
	}, nil
}

func (a *awsKinesisSink) Start() error {
	return nil
}

func (a *awsKinesisSink) Stop() error {
	return nil
}

func (a *awsKinesisSink) Emit(
	_ sink.Context, _ time.Time, topicName string, _, envelope schema.Struct,
) error {

	envelopeData, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = a.awsKinesis.PutRecord(&kinesis.PutRecordInput{
		StreamName:   a.streamName,
		PartitionKey: aws.String(topicName),
		Data:         envelopeData,
	})
	if err != nil {
		return err
	}

	return nil
}
