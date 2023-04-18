package awskinesis

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.AwsKinesis, newAwsKinesisSink)
}

type awsKinesisSink struct {
	streamName *string
	awsKinesis *kinesis.Kinesis
}

func newAwsKinesisSink(config *spiconfig.Config) (sink.Sink, error) {
	awsConfig := aws.NewConfig()

	streamName := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisStreamName, nil)
	if streamName == nil {
		return nil, errors.Errorf("AWS Kinesis sink needs the stream name to be configured")
	}

	shardCount := spiconfig.GetOrDefault[*int64](config, spiconfig.PropertyKinesisStreamShardCount, nil)
	streamMode := spiconfig.GetOrDefault[*string](config, spiconfig.PropertyKinesisStreamMode, nil)
	streamCreate := spiconfig.GetOrDefault(config, spiconfig.PropertyKinesisStreamCreate, true)

	var streamModeDetails *kinesis.StreamModeDetails
	if streamMode != nil {
		streamModeDetails = &kinesis.StreamModeDetails{
			StreamMode: streamName,
		}
	}

	awsSession, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	awsKinesis := kinesis.New(awsSession, awsConfig)
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
		_, err = awsKinesis.CreateStream(&kinesis.CreateStreamInput{
			ShardCount:        shardCount,
			StreamModeDetails: streamModeDetails,
			StreamName:        supporting.AddrOf("streamName"),
		})
		if err != nil {
			return nil, err
		}
	}

	return &awsKinesisSink{
		streamName: streamName,
		awsKinesis: awsKinesis,
	}, nil
}

func (a *awsKinesisSink) Emit(context sink.Context, _ time.Time, topicName string, _, envelope schema.Struct) error {
	var sequenceNumberForOrdering *string
	if prevSequenceNumber, present := context.Attribute("PrevSequenceNumber"); present {
		sequenceNumberForOrdering = &prevSequenceNumber
	}

	envelopeData, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	output, err := a.awsKinesis.PutRecord(&kinesis.PutRecordInput{
		StreamName:                a.streamName,
		PartitionKey:              &topicName,
		Data:                      envelopeData,
		SequenceNumberForOrdering: sequenceNumberForOrdering,
	})
	if err != nil {
		return err
	}

	if output.SequenceNumber != nil {
		context.SetAttribute("PrevSequenceNumber", *output.SequenceNumber)
	}
	return nil
}
