package integration

import (
	stdctx "context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	inttest "github.com/noctarius/timescaledb-event-streamer/internal/testing"
	"github.com/noctarius/timescaledb-event-streamer/internal/testing/testrunner"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type IntegrationSnapshotTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationSnapshotTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationSnapshotTestSuite))
}

func (its *IntegrationSnapshotTestSuite) TestInitialSnapshot_Hypertable() {
	waiter := supporting.NewWaiterWithTimeout(time.Second * 30)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents() == 8640 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if err := waiter.Await(); err != nil {
				return err
			}

			if len(testSink.Events()) < 8640 {
				its.T().Errorf(
					"not enough events received, expected: %d, received: %d", 8640, len(testSink.Events()),
				)
			}

			for i, event := range testSink.Events() {
				expected := i + 1
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour*24,
				systemcatalog.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, nil),
				systemcatalog.NewColumn("val", pgtype.Int4OID, "integer", false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-30 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Snapshot.Initial = supporting.AddrOf(spiconfig.InitialOnly)
				config.PostgreSQL.Snapshot.BatchSize = 100
			})
			return nil
		}),
	)
}
