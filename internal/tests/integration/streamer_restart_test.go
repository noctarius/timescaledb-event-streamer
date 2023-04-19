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
	"os"
	"testing"
	"time"
)

type IntegrationRestartTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationRestartTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationRestartTestSuite))
}

func (irts *IntegrationRestartTestSuite) Test_() {
	waiter := supporting.NewWaiterWithTimeout(time.Second * 30)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_CREATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents() == 1 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 21 {
				waiter.Signal()
			}
		}),
	)

	irts.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" (ts, val) VALUES ('2023-02-25 00:00:00', 1)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			if err := context.PauseReplicator(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) + 1 AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := context.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			for i, event := range testSink.Events() {
				expected := i + 1
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					irts.T().Errorf("event order inconsistent %d != %d", expected, val)
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

			tempFile, err := inttest.CreateTempFile("restart-replicator")
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tempFile", tempFile)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Config.PostgreSQL.ReplicationSlot.Name = supporting.RandomTextString(20)
				config.Config.PostgreSQL.ReplicationSlot.Create = supporting.AddrOf(true)
				config.Config.PostgreSQL.ReplicationSlot.AutoDrop = supporting.AddrOf(false)
				config.Config.PostgreSQL.Publication.Name = supporting.RandomTextString(10)
				config.Config.PostgreSQL.Publication.Create = supporting.AddrOf(true)
				config.Config.PostgreSQL.Publication.AutoDrop = supporting.AddrOf(false)
				config.Config.StateStorage.Type = spiconfig.FileStorage
				config.Config.StateStorage.FileStorage.Path = tempFile
			})
			return nil
		}),

		testrunner.WithTearDown(func(context testrunner.Context) error {
			tempFile := testrunner.GetAttribute[string](context, "tempFile")
			os.Remove(tempFile)
			return nil
		}),
	)
}
