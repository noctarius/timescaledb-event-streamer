package integration

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/testing/containers"
	"github.com/noctarius/timescaledb-event-streamer/internal/testing/testrunner"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"testing"
	"time"
)

type RedPandaIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestRedPandaIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(RedPandaIntegrationTestSuite))
}

func (kits *RedPandaIntegrationTestSuite) Test_RedPanda_Sink() {
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
				systemcatalog.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, nil),
				systemcatalog.NewColumn("val", pgtype.Int4OID, "integer", false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(setupContext, "schemaName", sn)
			testrunner.Attribute(setupContext, "tableName", tn)

			rC, brokers, err := containers.SetupRedPandaContainer()
			if err != nil {
				return errors.Wrap(err, 0)
			}
			container = rC
			testrunner.Attribute(setupContext, "brokers", brokers)

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.Kafka
				config.Sink.Kafka = spiconfig.KafkaConfig{
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
