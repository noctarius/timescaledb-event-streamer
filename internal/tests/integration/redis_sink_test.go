package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/go-redis/redis"
	"github.com/jackc/pgx/v5/pgtype"
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

type RedisIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestRedisIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(RedisIntegrationTestSuite))
}

func (rits *RedisIntegrationTestSuite) Test_Redis_Sink() {
	topicPrefix := supporting.RandomTextString(10)

	redisLogger, err := logging.NewLogger("Test_Redis_Sink")
	if err != nil {
		rits.T().Error(err)
	}

	var address string
	var container testcontainers.Container

	rits.RunTest(
		func(ctx testrunner.Context) error {
			client := redis.NewClient(&redis.Options{
				Addr: address,
			})

			subjectName := fmt.Sprintf(
				"%s.%s.%s", topicPrefix,
				testrunner.GetAttribute[string](ctx, "schemaName"),
				testrunner.GetAttribute[string](ctx, "tableName"),
			)

			groupName := supporting.RandomTextString(10)
			consumerName := supporting.RandomTextString(10)

			if err := client.XGroupCreateMkStream(subjectName, groupName, "0").Err(); err != nil {
				return err
			}

			collected := make(chan bool, 1)
			envelopes := make([]inttest.Envelope, 0)
			go func() {
				for {
					results, err := client.XReadGroup(&redis.XReadGroupArgs{
						Group:    groupName,
						Consumer: consumerName,
						Streams:  []string{subjectName, ">"},
						Count:    1,
						Block:    0,
						NoAck:    false,
					}).Result()
					if err != nil {
						redisLogger.Errorf("failed reading from redis: %+v", err)
						collected <- true
						return
					}

					for _, message := range results[0].Messages {
						envelope := inttest.Envelope{}
						if err := json.Unmarshal([]byte(message.Values["envelope"].(string)), &envelope); err != nil {
							rits.T().Error(err)
						}

						redisLogger.Debugf("EVENT: %+v", envelope)
						envelopes = append(envelopes, envelope)
						if len(envelopes) >= 10 {
							collected <- true
							return
						}

						client.XAck(subjectName, groupName, message.ID)
					}
				}
			}()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			for i, envelope := range envelopes {
				assert.Equal(rits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
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

			rC, rA, err := containers.SetupRedisContainer()
			if err != nil {
				return errors.Wrap(err, 0)
			}
			address = rA
			container = rC

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.Redis
				config.Sink.Redis = spiconfig.RedisConfig{
					Address: address,
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
