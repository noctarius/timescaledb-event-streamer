package awskinesis

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func IGNORE_Test_AWS_Kinesis_Config_Loading(t *testing.T) {
	config := &spiconfig.Config{
		Sink: spiconfig.SinkConfig{
			Type: "kinesis",
			AwsKinesis: spiconfig.AwsKinesisConfig{
				Stream: spiconfig.AwsKinesisStreamConfig{
					Name:       supporting.AddrOf("stream_name"),
					Create:     supporting.AddrOf(true),
					ShardCount: supporting.AddrOf(int64(100)),
					Mode:       supporting.AddrOf("stream_mode"),
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
