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
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_AWS_SQS_Config_Loading(
	t *testing.T,
) {

	config := &spiconfig.Config{
		Sink: spiconfig.SinkConfig{
			Type: "sqs",
			AwsSqs: spiconfig.AwsSqsConfig{
				Queue: spiconfig.AwsSqsQueueConfig{
					Url: lo.ToPtr("https://test_url"),
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

	sink, err := newAwsSqsSink(config)
	if err != nil {
		t.Error(err)
	}

	awsSink := sink.(*awsSqsSink)
	assert.Equal(t, "https://test_url", *awsSink.queueUrl)

	credentials, err := awsSink.awsSqs.Config.Credentials.Get()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "aws_region", *awsSink.awsSqs.Config.Region)
	assert.Equal(t, "aws_access_key_id", credentials.AccessKeyID)
	assert.Equal(t, "aws_secret_access_key", credentials.SecretAccessKey)
	assert.Equal(t, "aws_session_token", credentials.SessionToken)
}
