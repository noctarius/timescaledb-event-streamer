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
			if _, err := ctx.Exec(context.Background(), fmt.Sprintf(`
				CREATE OR REPLACE PROCEDURE insert_random_datapoints() 
				LANGUAGE plpgsql AS $$
				DECLARE 
				BEGIN
				LOOP
					CALL insert_one_datapoint();
					PERFORM PG_SLEEP(1);
				END LOOP;
				END;
				$$;

				CREATE OR REPLACE PROCEDURE insert_one_datapoint()
				LANGUAGE plpgsql AS $$
				DECLARE
				BEGIN
					INSERT INTO "%s" (ts, val) VALUES (NOW(), RANDOM() * 100);
					COMMIT;
				END;
				$$;`, testrunner.GetAttribute[string](ctx, "tableName")),
			); err != nil {
				return err
			}

			go func() {
				if _, err := ctx.Exec(context.Background(),
					"CALL insert_random_datapoints();",
				); err != nil {
					irts.Logger().Warnf("Inner go routine killed: %s", err)
				}
			}()

			if err := waiter.Await(); err != nil {
				return err
			}

			irts.StopContainer()

			if err := ctx.PauseReplicator(); err != nil {
				return err
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

			if _, err := ctx.Exec(context.Background(), fmt.Sprintf("ALTER table \"%s\" REPLICA IDENTITY FULL", tn)); err != nil {
				return err
			}

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
