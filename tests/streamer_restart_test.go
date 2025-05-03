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

type IntegrationRestartTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationRestartTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(IntegrationRestartTestSuite))
}

func (irts *IntegrationRestartTestSuite) Test_Restart_Streamer() {
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

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) + 1 AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := ctx.ResumeReplicator(); err != nil {
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
				config.PostgreSQL.ReplicationSlot.Name = lo.RandomString(20, lo.LowerCaseLettersCharset)
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

func (irts *IntegrationRestartTestSuite) Test_Restart_Streamer_After_Backend_Kill() {
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

			if err := ctx.PrivilegedContext(func(ctx testrunner.PrivilegedContext) error {
				_, err := ctx.Query(
					context.Background(),
					"SELECT pg_terminate_backend(rs.active_pid) FROM pg_catalog.pg_replication_slots rs WHERE rs.slot_name = $1",
					replicationSlotName,
				)
				return err
			}); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) + 1 AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := ctx.ResumeReplicator(); err != nil {
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

func (irts *IntegrationRestartTestSuite) Test_Restart_Streamer_Without_Restart_LSN() {
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

			if err := ctx.PrivilegedContext(func(ctx testrunner.PrivilegedContext) error {
				_, err := ctx.Query(
					context.Background(),
					"SELECT pg_replication_slot_advance($1, NULL)",
					replicationSlotName,
				)
				return err
			}); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}
			waiter.Reset()

			time.Sleep(time.Second * 10)

			if err := ctx.ResumeReplicator(); err != nil {
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
