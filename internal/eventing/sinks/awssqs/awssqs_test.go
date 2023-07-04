package awssqs

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_AWS_SQS_Config_Loading(t *testing.T) {
	config := &spiconfig.Config{
		Sink: spiconfig.SinkConfig{
			Type: "sqs",
			AwsSqs: spiconfig.AwsSqsConfig{
				Queue: spiconfig.AwsSqsQueueConfig{
					Url: supporting.AddrOf("https://test_url"),
				},
				Aws: spiconfig.AwsConnectionConfig{
					Region:          supporting.AddrOf("aws_region"),
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
