package tests

import (
	"context"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
	"time"
)

type IntegrationDatabaseGoneTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationDatabaseGoneTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(IntegrationDatabaseGoneTestSuite))
}

func (irts *IntegrationDatabaseGoneTestSuite) Test_Streamer_Shutdown_After_Container_Stop() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 60)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() == 1 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 21 {
				waiter.Signal()
			}
		}),
	)

	replicationSlotName := lo.RandomString(20, lo.LowerCaseLettersCharset)

	irts.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" (ts, val) VALUES ('2023-02-25 00:00:00', 1)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			irts.TestRunner.StopContainer()

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tableName", tn)

			tempFile, err := testsupport.CreateTempFile("restart-replicator")
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tempFile", tempFile)

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.Create = lo.ToPtr(true)
				config.PostgreSQL.ReplicationSlot.AutoDrop = lo.ToPtr(false)
				config.PostgreSQL.Publication.Name = lo.RandomString(10, lo.LowerCaseLettersCharset)
				config.PostgreSQL.Publication.Create = lo.ToPtr(true)
				config.PostgreSQL.Publication.AutoDrop = lo.ToPtr(false)
				config.StateStorage.Type = spiconfig.FileStorage
				config.StateStorage.FileStorage.Path = tempFile
			})
			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			tempFile := testrunner.GetAttribute[string](ctx, "tempFile")
			os.Remove(tempFile)
			return nil
		}),
	)
}
