/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

type IntegrationSnapshotTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationSnapshotTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(IntegrationSnapshotTestSuite))
}

func (its *IntegrationSnapshotTestSuite) TestInitialSnapshot_Hypertable() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 60)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				time.Sleep(time.Millisecond)
				return envelope.Payload.Op == schema.OP_READ
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() == 8640 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if err := waiter.Await(); err != nil {
				its.T().Logf("Expected 8640 events but only received %d", len(testSink.Events()))
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

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tableName", tn)

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-30 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Snapshot.Initial = lo.ToPtr(spiconfig.InitialOnly)
				config.PostgreSQL.Snapshot.BatchSize = 100
			})
			return nil
		}),
	)
}

func (its *IntegrationSnapshotTestSuite) TestInitialSnapshot_Vanilla_Table() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 60)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				time.Sleep(time.Millisecond)
				return envelope.Payload.Op == schema.OP_READ
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() == 4320 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 8640 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if err := waiter.Await(); err != nil {
				its.T().Logf("Expected 4320 events but only received %d", len(testSink.Events()))
				return err
			}
			if err := ctx.PauseReplicator(); err != nil {
				return err
			}
			waiter.Reset()
			if err := ctx.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				its.T().Logf("Expected 8640 events but only received %d", len(testSink.Events()))
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

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateVanillaTable(
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tableName", tn)

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-30 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			tempFile, err := testsupport.CreateTempFile("restart-replicator")
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tempFile", tempFile)

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Snapshot.Initial = lo.ToPtr(spiconfig.InitialOnly)
				config.PostgreSQL.Snapshot.BatchSize = 100
				config.Config.PostgreSQL.ReplicationSlot.Name = lo.RandomString(20, lo.LowerCaseLettersCharset)
				config.Config.PostgreSQL.ReplicationSlot.Create = lo.ToPtr(true)
				config.Config.PostgreSQL.ReplicationSlot.AutoDrop = lo.ToPtr(false)
				config.Config.PostgreSQL.Publication.Name = lo.RandomString(10, lo.LowerCaseLettersCharset)
				config.Config.PostgreSQL.Publication.Create = lo.ToPtr(true)
				config.Config.PostgreSQL.Publication.AutoDrop = lo.ToPtr(false)
				config.Config.StateStorage.Type = spiconfig.FileStorage
				config.Config.StateStorage.FileStorage.Path = tempFile
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
