package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/nats-io/nats.go"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	inttest "github.com/noctarius/timescaledb-event-streamer/internal/testing"
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

type NatsIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestNatsIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(NatsIntegrationTestSuite))
}

func (nits *NatsIntegrationTestSuite) Test_Nats_Sink() {
	topicPrefix := supporting.RandomTextString(10)

	natsLogger, err := logging.NewLogger("Test_Nats_Sink")
	if err != nil {
		nits.T().Error(err)
	}

	var natsUrl string
	var natsContainer testcontainers.Container

	nits.RunTest(
		func(ctx testrunner.Context) error {
			// Collect logs
			natsContainer.FollowOutput(&testrunner.ContainerLogForwarder{Logger: natsLogger})
			natsContainer.StartLogProducer(context.Background())

			conn, err := nats.Connect(natsUrl, nats.DontRandomize(), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
			if err != nil {
				return err
			}

			js, err := conn.JetStream(nats.PublishAsyncMaxPending(256))
			if err != nil {
				return err
			}

			subjectName := fmt.Sprintf(
				"%s.%s.%s", topicPrefix,
				testrunner.GetAttribute[string](ctx, "schemaName"),
				testrunner.GetAttribute[string](ctx, "tableName"),
			)

			streamName := supporting.RandomTextString(10)
			groupName := supporting.RandomTextString(10)

			natsLogger.Println("Creating NATS JetStream stream...")
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{subjectName},
			})
			if err != nil {
				return err
			}

			waiter := supporting.NewWaiterWithTimeout(time.Minute)
			envelopes := make([]inttest.Envelope, 0)
			_, err = js.QueueSubscribe(subjectName, groupName, func(msg *nats.Msg) {
				envelope := inttest.Envelope{}
				if err := json.Unmarshal(msg.Data, &envelope); err != nil {
					msg.Nak()
					nits.T().Error(err)
				}
				natsLogger.Debugf("EVENT: %+v", envelope)
				envelopes = append(envelopes, envelope)
				if len(envelopes) >= 10 {
					waiter.Signal()
				}
				msg.Ack()
			}, nats.ManualAck())
			if err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			for i, envelope := range envelopes {
				assert.Equal(nits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
			}
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			sn, tn, err := setupContext.CreateHypertable("ts", time.Hour*24,
				systemcatalog.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				systemcatalog.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(setupContext, "schemaName", sn)
			testrunner.Attribute(setupContext, "tableName", tn)

			nC, nU, err := containers.SetupNatsContainer()
			if err != nil {
				return errors.Wrap(err, 0)
			}
			natsUrl = nU
			natsContainer = nC

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.NATS
				config.Sink.Nats = spiconfig.NatsConfig{
					Address:       natsUrl,
					Authorization: spiconfig.UserInfo,
					UserInfo: spiconfig.NatsUserInfoConfig{
						Username: "",
						Password: "",
					},
				}
			})

			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			if natsContainer != nil {
				natsContainer.Terminate(context.Background())
			}
			return nil
		}),
	)
}
