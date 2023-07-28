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
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func IGNORE_Test_AWS_Kinesis_Config_Loading(
	t *testing.T,
) {

	config := &spiconfig.Config{
		Sink: spiconfig.SinkConfig{
			Type: "kinesis",
			AwsKinesis: spiconfig.AwsKinesisConfig{
				Stream: spiconfig.AwsKinesisStreamConfig{
					Name:       lo.ToPtr("stream_name"),
					Create:     lo.ToPtr(true),
					ShardCount: lo.ToPtr(int64(100)),
					Mode:       lo.ToPtr("stream_mode"),
				},
				Aws: spiconfig.AwsConnectionConfig{
					Region:          lo.ToPtr("aws_region"),
					Endpoint:        "aws_endpoint",
					AccessKeyId:     "aws_access_key_id",
					SecretAccessKey: "aws_secret_access_key",
					SessionToken:    "aws_session_token",
				},
			},
		},
	}

	sink, err := newAwsKinesisSink(config)
	if err != nil {
		t.Error(err)
	}

	awsSink := sink.(*awsKinesisSink)
	assert.Equal(t, "stream_name", *awsSink.streamName)

	credentials, err := awsSink.awsKinesis.Config.Credentials.Get()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "aws_region", *awsSink.awsKinesis.Config.Region)
	assert.Equal(t, "aws_access_key_id", credentials.AccessKeyID)
	assert.Equal(t, "aws_secret_access_key", credentials.SecretAccessKey)
	assert.Equal(t, "aws_session_token", credentials.SessionToken)
}
