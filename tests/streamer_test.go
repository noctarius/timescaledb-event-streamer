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
	"encoding/base64"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type IntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(IntegrationTestSuite))
}

func (its *IntegrationTestSuite) Test_Hypertable_InitialSnapshot_Single_Chunk() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 30)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() == 1440 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_InitialSnapshot_Multi_Chunk() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 60)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() == 2880 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-26 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Create_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:10:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type create (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'c' but was %s", event.Envelope.Payload.Op)
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Update_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"UPDATE \"%s\" SET val = val + 10",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type update (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 11
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_UPDATE {
					its.T().Errorf("event should be of type 'u' but was %s", event.Envelope.Payload.Op)
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Delete_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"DELETE FROM \"%s\"",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type delete (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				event := testSink.Events()[i+10]
				if event.Envelope.Payload.Op != schema.OP_DELETE {
					its.T().Errorf("event should be of type 'd' but was %s", event.Envelope.Payload.Op)
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Truncate_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_TRUNCATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 11 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"TRUNCATE %s",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Final event must be a truncate event
			event := testSink.Events()[10]
			if event.Envelope.Payload.Op != schema.OP_TRUNCATE {
				its.T().Errorf("event should be of type 't' but was %s", event.Envelope.Payload.Op)
				return nil
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Compression_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_TIMESCALE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 11 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"ALTER TABLE \"%s\" SET (timescaledb.compress)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT compress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s') t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Final event must be a truncate event
			event := testSink.Events()[10]
			if event.Envelope.Payload.Op != schema.OP_TIMESCALE {
				its.T().Errorf("event should be of type '$' but was %s", event.Envelope.Payload.Op)
				return nil
			}
			if event.Envelope.Payload.TsdbOp != schema.OP_COMPRESSION {
				its.T().Errorf("event should be of timescaledb type 'c' but was %s", event.Envelope.Payload.TsdbOp)
				return nil
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Events.Compression = lo.ToPtr(true)
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Compression_Partial_Insert_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ ||
					envelope.Payload.Op == schema.OP_CREATE ||
					envelope.Payload.Op == schema.OP_TIMESCALE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 13 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"ALTER TABLE \"%s\" SET (timescaledb.compress)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT compress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s') t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" VALUES ('2023-03-25 00:10:59', 5555)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT decompress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s' AND is_compressed) t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Final event must be a truncate event
			event := testSink.Events()[10]
			if event.Envelope.Payload.Op != schema.OP_TIMESCALE {
				its.T().Errorf("event should be of type '$' but was %s", event.Envelope.Payload.Op)
				return nil
			}
			if event.Envelope.Payload.TsdbOp != schema.OP_COMPRESSION {
				its.T().Errorf("event should be of timescaledb type 'c' but was %s", event.Envelope.Payload.TsdbOp)
				return nil
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Events.Compression = lo.ToPtr(true)
				config.TimescaleDB.Events.Decompression = lo.ToPtr(true)
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Decompression_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_TIMESCALE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, envelope testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}

			if envelope.Payload.Op == schema.OP_TIMESCALE &&
				(envelope.Payload.TsdbOp == schema.OP_DECOMPRESSION ||
					envelope.Payload.TsdbOp == schema.OP_COMPRESSION) {

				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"ALTER TABLE \"%s\" SET (timescaledb.compress)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT compress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s') t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT decompress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s' AND is_compressed) t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Final event must be a truncate event
			event := testSink.Events()[10]
			if event.Envelope.Payload.Op != schema.OP_TIMESCALE {
				its.T().Errorf("event should be of type '$' but was %s", event.Envelope.Payload.Op)
				return nil
			}
			if event.Envelope.Payload.TsdbOp != schema.OP_COMPRESSION {
				its.T().Errorf("event should be of timescaledb type 'c' but was %s", event.Envelope.Payload.TsdbOp)
				return nil
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Events.Compression = lo.ToPtr(true)
				config.TimescaleDB.Events.Decompression = lo.ToPtr(true)
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Compression_Decompression_SingleTransaction_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_TIMESCALE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, envelope testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}

			if envelope.Payload.Op == schema.OP_TIMESCALE &&
				envelope.Payload.TsdbOp == schema.OP_DECOMPRESSION {

				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			tx, err := ctx.Begin(context.Background())
			if err != nil {
				return err
			}
			if _, err := tx.Exec(context.Background(),
				fmt.Sprintf(
					"ALTER TABLE \"%s\" SET (timescaledb.compress)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := tx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT compress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s') t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := tx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT decompress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s' AND is_compressed) t",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}
			if err := tx.Commit(context.Background()); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Final event must be a truncate event
			event := testSink.Events()[10]
			if event.Envelope.Payload.Op != schema.OP_TIMESCALE {
				its.T().Errorf("event should be of type '$' but was %s", event.Envelope.Payload.Op)
				return nil
			}
			if event.Envelope.Payload.TsdbOp != schema.OP_COMPRESSION {
				its.T().Errorf("event should be of timescaledb type 'c' but was %s", event.Envelope.Payload.TsdbOp)
				return nil
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Events.Compression = lo.ToPtr(true)
				config.TimescaleDB.Events.Decompression = lo.ToPtr(true)
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_ContinuousAggregate_Create_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%20 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"CALL refresh_continuous_aggregate('%s', '2023-03-25','2023-03-26')",
					testrunner.GetAttribute[string](ctx, "aggregateName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			for i := 0; i < 20; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'c' but was %s", event.Envelope.Payload.Op)
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

			aggregateName := lo.RandomString(10, lo.LowerCaseLettersCharset)
			testrunner.Attribute(ctx, "aggregateName", aggregateName)

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous) AS SELECT time_bucket('1 min', t.ts) bucket, max(val) val FROM %s t GROUP BY 1",
					aggregateName, tn,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Hypertables.Includes = []string{
					systemcatalog.MakeRelationKey(testsupport.DatabaseSchema, aggregateName),
				}
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_ContinuousAggregate_Scheduled_Refresh_Create_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 30)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%20 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			logger, err := logging.NewLogger("CAGG_REFRESH_TEST")
			if err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			waiter.Reset()
			logger.Warnln("Scheduling continuous aggregate refresh")
			if err := ctx.PrivilegedContext(func(pctx testrunner.PrivilegedContext) error {
				_, err := pctx.Exec(context.Background(), `
					SELECT tsdb.alter_job(j.id, next_start => now() + interval '5 seconds')
					FROM _timescaledb_config.bgw_job j
					LEFT JOIN _timescaledb_catalog.hypertable h ON h.id = j.hypertable_id
					LEFT JOIN _timescaledb_catalog.continuous_agg c ON c.mat_hypertable_id = j.hypertable_id
					WHERE c.user_view_name = $1`,
					testrunner.GetAttribute[string](ctx, "aggregateName"),
				)
				return err
			}); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			for i := 0; i < 20; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'c' but was %s", event.Envelope.Payload.Op)
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

			aggregateName := lo.RandomString(10, lo.LowerCaseLettersCharset)
			testrunner.Attribute(ctx, "aggregateName", aggregateName)

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous) AS SELECT time_bucket('1 min', t.ts) bucket, max(val) val FROM %s t GROUP BY 1 WITH NO DATA",
					aggregateName, tn,
				),
			); err != nil {
				return err
			}
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"SELECT add_continuous_aggregate_policy('%s', NULL, NULL, schedule_interval => interval '1 day')",
					aggregateName,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Hypertables.Includes = []string{
					systemcatalog.MakeRelationKey(testsupport.DatabaseSchema, aggregateName),
				}
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_General_Emit_Logical_Message() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_MESSAGE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() == 1 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			pgVersion := ctx.PostgresqlVersion()
			if pgVersion < version.PG_14_VERSION {
				fmt.Printf("Skipped test, because of PostgreSQL version <14.0 (%s)", pgVersion)
				return nil
			}

			tx, err := ctx.Begin(context.Background())
			if err != nil {
				return err
			}
			if _, err := tx.Exec(context.Background(),
				"SELECT pg_logical_emit_message(true, 'test-prefix', 'this is a replication message')",
			); err != nil {
				return err
			}
			if err := tx.Commit(context.Background()); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			event := testSink.Events()[0]
			assert.NotNil(its.T(), event.Envelope.Payload.Message)
			assert.Equal(its.T(), event.Envelope.Payload.Message.Prefix, "test-prefix")
			assert.Equal(its.T(), event.Envelope.Payload.Message.Content, "dGhpcyBpcyBhIHJlcGxpY2F0aW9uIG1lc3NhZ2U=")
			d, err := base64.StdEncoding.DecodeString(event.Envelope.Payload.Message.Content)
			if err != nil {
				return err
			}
			assert.Equal(its.T(), "this is a replication message", string(d))
			if event.Envelope.Payload.Op != schema.OP_MESSAGE {
				its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
				return nil
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_General_Acknowledge_To_PG_With_Only_Begin_Commit() {
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
		}),
	)

	replicationSlotName := lo.RandomString(20, lo.LowerCaseLettersCharset)
	its.RunTest(
		func(ctx testrunner.Context) error {
			pgVersion := ctx.PostgresqlVersion()
			if pgVersion >= version.PG_15_VERSION {
				fmt.Printf("Skipped test, because of PostgreSQL version <15.0 (%s)", pgVersion)
				return nil
			}

			tableName := testrunner.GetAttribute[string](ctx, "tableName")

			var lsn1 pglogrepl.LSN
			if err := ctx.QueryRow(context.Background(),
				"SELECT confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
				replicationSlotName,
			).Scan(&lsn1); err != nil {
				return err
			}

			for i := 0; i < 100; i++ {
				if _, err := ctx.Exec(context.Background(), "INSERT INTO tsdb.foo VALUES($1)", i); err != nil {
					return err
				}
				time.Sleep(time.Millisecond * 5)
			}

			fmt.Print("Waiting for status update to server\n")
			time.Sleep(time.Second * 6)

			var lsn2 pglogrepl.LSN
			if err := ctx.QueryRow(context.Background(),
				"SELECT confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
				replicationSlotName,
			).Scan(&lsn2); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" VALUES('2023-03-25 00:00:01', 654)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			if lsn2 <= lsn1 {
				its.T().Errorf(
					"LSN2 must be larger than LSN1 - LSN1: %s, LSN2: %s", lsn1, lsn2,
				)
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour*6,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tableName", tn)

			if _, err := ctx.Exec(context.Background(), "CREATE TABLE tsdb.foo (val int)"); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tn,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
			})
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Vanilla_Create_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:10:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type create (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'c' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateVanillaTable(
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tableName", tn)

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Vanilla_Update_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"UPDATE \"%s\" SET val = val + 10",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type update (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 11
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_UPDATE {
					its.T().Errorf("event should be of type 'u' but was %s", event.Envelope.Payload.Op)
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Vanilla_Delete_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"DELETE FROM \"%s\"",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type delete (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				event := testSink.Events()[i+10]
				if event.Envelope.Payload.Op != schema.OP_DELETE {
					its.T().Errorf("event should be of type 'd' but was %s", event.Envelope.Payload.Op)
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

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Vanilla_Truncate_Events() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_TRUNCATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
			if sink.NumOfEvents() == 11 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
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
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"TRUNCATE %s",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Final event must be a truncate event
			event := testSink.Events()[10]
			if event.Envelope.Payload.Op != schema.OP_TRUNCATE {
				its.T().Errorf("event should be of type 't' but was %s", event.Envelope.Payload.Op)
				return nil
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateVanillaTable(
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(ctx, "tableName", tn)

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Replica_Identity_Full_Update_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			tsdbVersion := ctx.TimescaleVersion()
			if tsdbVersion < version.TSDB_212_VERSION {
				fmt.Printf("Skipped test, because of TimescaleDB version <2.12 (%s)", tsdbVersion)
				return nil
			}

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"UPDATE \"%s\" SET val = val + 10",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type update (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 11
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				val = int(event.Envelope.Payload.Before["val"].(float64))
				if expected-10 != val {
					its.T().Errorf("event before value unexpected %d != %d", expected-10, val)
					return nil
				}
				assert.Equal(its.T(), event.Envelope.Payload.After["ts"], event.Envelope.Payload.Before["ts"])
				if event.Envelope.Payload.Op != schema.OP_UPDATE {
					its.T().Errorf("event should be of type 'u' but was %s", event.Envelope.Payload.Op)
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
			tableName = tn

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Hypertable_Replica_Identity_Full_Delete_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			tsdbVersion := ctx.TimescaleVersion()
			if tsdbVersion < version.TSDB_212_VERSION {
				fmt.Printf("Skipped test, because of TimescaleDB version <2.12 (%s)", tsdbVersion)
				return nil
			}

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"DELETE FROM \"%s\"", tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type delete (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.Before["val"].(float64))
				if i+1 != val {
					its.T().Errorf("event before value unexpected %d != %d", i+1, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_DELETE {
					its.T().Errorf("event should be of type 'd' but was %s", event.Envelope.Payload.Op)
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
			tableName = tn

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Ignore_Test_Hypertable_Replica_Identity_Index_Update_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			tsdbVersion := ctx.TimescaleVersion()
			if tsdbVersion < version.TSDB_212_VERSION {
				fmt.Printf("Skipped test, because of TimescaleDB version <2.12 (%s)", tsdbVersion)
				return nil
			}

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s_pk", tableName, tableName),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS key, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"UPDATE \"%s\" SET val = val + 10",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type update (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 11
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				val = int(event.Envelope.Payload.Before["key"].(float64))
				if expected-10 != val {
					its.T().Errorf("event before key unexpected %d != %d", expected-10, val)
					return nil
				}
				assert.Equal(its.T(), event.Envelope.Payload.After["ts"], event.Envelope.Payload.Before["ts"])
				if event.Envelope.Payload.Op != schema.OP_UPDATE {
					its.T().Errorf("event should be of type 'u' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("key", "integer", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("CREATE UNIQUE INDEX %s_pk ON %s (ts, key)", tn, tn),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Ignore_Test_Hypertable_Replica_Identity_Index_Delete_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			tsdbVersion := ctx.TimescaleVersion()
			if tsdbVersion < version.TSDB_212_VERSION {
				fmt.Printf("Skipped test, because of TimescaleDB version <2.12 (%s)", tsdbVersion)
				return nil
			}

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s_pk", tableName, tableName),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS key, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"DELETE FROM \"%s\"", tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type delete (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.Before["key"].(float64))
				if i+1 != val {
					its.T().Errorf("event before value unexpected %d != %d", i+1, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_DELETE {
					its.T().Errorf("event should be of type 'd' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("key", "integer", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("CREATE UNIQUE INDEX %s_pk ON %s (ts, key)", tn, tn),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Vanilla_Replica_Identity_Full_Update_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"UPDATE \"%s\" SET val = val + 10",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type update (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 11
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				val = int(event.Envelope.Payload.Before["val"].(float64))
				if expected-10 != val {
					its.T().Errorf("event before value unexpected %d != %d", expected-10, val)
					return nil
				}
				assert.Equal(its.T(), event.Envelope.Payload.After["ts"], event.Envelope.Payload.Before["ts"])
				if event.Envelope.Payload.Op != schema.OP_UPDATE {
					its.T().Errorf("event should be of type 'u' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateVanillaTable(
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Test_Vanilla_Replica_Identity_Full_Delete_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"DELETE FROM \"%s\"", tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type delete (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.Before["val"].(float64))
				if i+1 != val {
					its.T().Errorf("event before value unexpected %d != %d", i+1, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_DELETE {
					its.T().Errorf("event should be of type 'd' but was %s", event.Envelope.Payload.Op)
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
			tableName = tn

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Ignore_Test_Vanilla_Replica_Identity_Index_Update_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS key, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"UPDATE \"%s\" SET val = val + 10",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type update (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				expected := i + 11
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				val = int(event.Envelope.Payload.Before["key"].(float64))
				if expected-10 != val {
					its.T().Errorf("event before key unexpected %d != %d", expected-10, val)
					return nil
				}
				assert.Equal(its.T(), event.Envelope.Payload.After["ts"], event.Envelope.Payload.Before["ts"])
				if event.Envelope.Payload.Op != schema.OP_UPDATE {
					its.T().Errorf("event should be of type 'u' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateVanillaTable(
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("key", "integer", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("CREATE UNIQUE INDEX %s_pk ON %s (ts, key)", tn, tn),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s_pk", tableName, tableName),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) Ignore_Test_Vanilla_Replica_Identity_Index_Delete_Events() {
	var tableName string

	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents()%10 == 0 {
				waiter.Signal()
			}
		}),
	)

	its.RunTest(
		func(ctx testrunner.Context) error {
			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS key, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}
			waiter.Reset()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"DELETE FROM \"%s\"", tableName,
				),
			); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_CREATE {
					its.T().Errorf("event should be of type 'r' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			// Next 10 events have to be of type delete (chunk should already be replicated)
			for i := 0; i < 10; i++ {
				event := testSink.Events()[i+10]
				val := int(event.Envelope.Payload.Before["key"].(float64))
				if i+1 != val {
					its.T().Errorf("event before value unexpected %d != %d", i+1, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_DELETE {
					its.T().Errorf("event should be of type 'd' but was %s", event.Envelope.Payload.Op)
					return nil
				}
			}

			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateVanillaTable(
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn("key", "integer", false, false, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("CREATE UNIQUE INDEX %s_pk ON %s (ts, key)", tn, tn),
			); err != nil {
				return err
			}

			if _, err := ctx.Exec(
				context.Background(),
				fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s_pk", tableName, tableName),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}
