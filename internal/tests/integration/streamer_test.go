package integration

import (
	stdctx "context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	inttest "github.com/noctarius/event-stream-prototype/internal/testing"
	"github.com/noctarius/event-stream-prototype/internal/testing/testrunner"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type IntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (its *IntegrationTestSuite) TestInitialSnapshot_Single_Chunk() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents() == 1440 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

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
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) TestInitialSnapshot_Multi_Chunk() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents() == 2880 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-26 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

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
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) TestCreateEvents() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_CREATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents()%10 == 0 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:10:00'::TIMESTAMPTZ, '2023-03-25 00:19:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_READ {
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

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour*24,
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) TestUpdateEvents() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_UPDATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents()%10 == 0 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"UPDATE %s SET val = val + 10",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_READ {
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

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour*24,
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, true, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) TestDeleteEvents() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_DELETE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents()%10 == 0 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"DELETE FROM %s",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_READ {
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

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour*24,
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, true, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) TestTruncateEvents() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_TRUNCATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents()%10 == 0 {
				collected <- true
			}
			if sink.NumOfEvents() == 11 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"TRUNCATE %s",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_READ {
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

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour*24,
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

func (its *IntegrationTestSuite) TestCompressionEvents() {
	collected := make(chan bool, 1)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_READ || envelope.Payload.Op == schema.OP_TIMESCALE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents()%10 == 0 {
				collected <- true
			}
			if sink.NumOfEvents() == 11 {
				collected <- true
			}
		}),
	)

	its.RunTest(
		func(context testrunner.Context) error {
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO %s SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"ALTER TABLE %s SET (timescaledb.compress)",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"SELECT compress_chunk((t.chunk_schema || '.' || t.chunk_name)::regclass, true) FROM (SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = '%s') t",
					testrunner.GetAttribute[string](context, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			// Initial 10 events have to be of type read (same transaction as the chunk creation)
			for i := 0; i < 10; i++ {
				expected := i + 1
				event := testSink.Events()[i]
				val := int(event.Envelope.Payload.After["val"].(float64))
				if expected != val {
					its.T().Errorf("event order inconsistent %d != %d", expected, val)
					return nil
				}
				if event.Envelope.Payload.Op != schema.OP_READ {
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

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour*24,
				model.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, false, nil),
				model.NewColumn("val", pgtype.Int4OID, "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(context, "tableName", tn)

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.TimescaleDB.Events.Compression = true
			})
			return nil
		}),
	)
}