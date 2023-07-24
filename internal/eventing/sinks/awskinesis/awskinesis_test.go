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
