package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	inttest "github.com/noctarius/event-stream-prototype/internal/testing"
	"github.com/noctarius/event-stream-prototype/internal/testing/containers"
	"github.com/noctarius/event-stream-prototype/internal/testing/testrunner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"testing"
	"time"
)

var kafkaLogger = logging.NewLogger("Test_Kafka_Sink")

type KafkaIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestKafkaIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(KafkaIntegrationTestSuite))
}

func (kits *KafkaIntegrationTestSuite) Test_Kafka_Sink() {
	topicPrefix := supporting.RandomTextString(10)

	var container testcontainers.Container

	kits.RunTest(
		func(ctx testrunner.Context) error {
			topicName := fmt.Sprintf(
				"%s.%s.%s", topicPrefix,
				testrunner.GetAttribute[string](ctx, "schemaName"),
				testrunner.GetAttribute[string](ctx, "tableName"),
			)

			groupName := supporting.RandomTextString(10)

			config := sarama.NewConfig()
			client, err := sarama.NewConsumerGroup(testrunner.GetAttribute[[]string](ctx, "brokers"), groupName, config)
			if err != nil {
				return err
			}

			consumer, ready := newKafkaConsumer(kits.T())
			go func() {
				if err := client.Consume(context.Background(), []string{topicName}, consumer); err != nil {
					kits.T().Error(err)
				}
			}()

			<-ready

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			<-consumer.collected

			for i, envelope := range consumer.envelopes {
				assert.Equal(kits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
			}
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			sn, tn, err := setupContext.CreateHypertable("ts", time.Hour*24,
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(setupContext, "schemaName", sn)
			testrunner.Attribute(setupContext, "tableName", tn)

			kC, brokers, err := containers.SetupKafkaContainer()
			if err != nil {
				return errors.Wrap(err, 0)
			}
			container = kC
			testrunner.Attribute(setupContext, "brokers", brokers)

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = configuring.Kafka
				config.Sink.Kafka = configuring.KafkaConfig{
					Brokers:    brokers,
					Idempotent: false,
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

type kafkaConsumer struct {
	t         *testing.T
	ready     chan bool
	collected chan bool
	envelopes []inttest.Envelope
}

func newKafkaConsumer(t *testing.T) (*kafkaConsumer, <-chan bool) {
	kc := &kafkaConsumer{
		t:         t,
		ready:     make(chan bool, 1),
		collected: make(chan bool, 1),
		envelopes: make([]inttest.Envelope, 0),
	}
	return kc, kc.ready
}

func (k *kafkaConsumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (k *kafkaConsumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (k *kafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	k.ready <- true
	for {
		select {
		case message := <-claim.Messages():
			envelope := inttest.Envelope{}
			if err := json.Unmarshal(message.Value, &envelope); err != nil {
				k.t.Error(err)
			}
			kafkaLogger.Printf("EVENT: %+v", envelope)
			k.envelopes = append(k.envelopes, envelope)
			if len(k.envelopes) >= 10 {
				k.collected <- true
			}
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
