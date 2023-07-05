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

type IntegrationLoadingSystemCatalogTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationLoadingSystemCatalogTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationLoadingSystemCatalogTestSuite))
}

func (its *IntegrationLoadingSystemCatalogTestSuite) Test_Loading_Large_System_Catalog() {
	waiter := supporting.NewWaiterWithTimeout(time.Second * 60)
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
			_, tn, err := context.CreateHypertable("ts", time.Second,
				systemcatalog.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, nil),
				systemcatalog.NewColumn("val", pgtype.Int4OID, "integer", false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 second') t(ts)",
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
