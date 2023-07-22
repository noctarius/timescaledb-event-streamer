package tests

import (
	stdctx "context"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	inttest "github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/stretchr/testify/suite"
	"os"
	"strings"
	"testing"
	"time"
)

const sqlReadAllChunksForHypertable = `
SELECT c.schema_name, c.table_name
FROM _timescaledb_catalog.chunk c
LEFT JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
WHERE h.table_name = $1`

const sqlReadPublishedChunks = `
SELECT n.nspname::text, c.relname
FROM pg_catalog.pg_publication p
LEFT JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid
LEFT JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE p.pubname = $1`

const sqlDropChunksFromPublication = `
ALTER PUBLICATION %s DROP TABLE %s 
`

type PublicationTestSuite struct {
	testrunner.TestRunner
}

func TestPublicationTestSuite(t *testing.T) {
	suite.Run(t, new(PublicationTestSuite))
}

func (pts *PublicationTestSuite) Test_Preexisting_Chunks_Added_To_Publication() {
	testSink := inttest.NewEventCollectorSink()
	publicationName := supporting.RandomTextString(10)

	var tableName string
	pts.RunTest(
		func(context testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			return nil
		},

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour,
				inttest.NewColumn("ts", "timestamptz", false, false, nil),
				inttest.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
			})
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Reloading_From_Known_Chunks() {
	waiter := supporting.NewWaiterWithTimeout(time.Second * 20)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			waiter.Signal()
		}),
	)

	publicationName := supporting.RandomTextString(10)
	replicationSlotName := supporting.RandomTextString(20)
	stateStorageFile := fmt.Sprintf("/tmp/%s", supporting.RandomTextString(10))

	var tableName string
	pts.RunTest(
		func(context testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			if err := context.PauseReplicator(); err != nil {
				return err
			}

			// Create a new chunk
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-26 00:00:00'::TIMESTAMPTZ, '2023-03-26 00:00:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			// Empty sink and reset the waiter
			testSink.Clear()
			waiter.Reset()

			if err := context.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			existingChunks, publishedChunks, err = readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			return nil
		},

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour,
				inttest.NewColumn("ts", "timestamptz", false, false, nil),
				inttest.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
				config.PostgreSQL.Publication.AutoDrop = supporting.AddrOf(false)

				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.AutoDrop = supporting.AddrOf(false)

				config.StateStorage.Type = spiconfig.FileStorage
				config.StateStorage.FileStorage = spiconfig.FileStorageConfig{
					Path: stateStorageFile,
				}
			})
			return nil
		}),

		testrunner.WithTearDown(func(context testrunner.Context) error {
			// If error happens, we don't care
			os.Remove(stateStorageFile)
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Fixing_Broken_Publications_With_State_Storage() {
	waiter := supporting.NewWaiterWithTimeout(time.Second * 20)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			waiter.Signal()
		}),
	)

	publicationName := supporting.RandomTextString(10)
	replicationSlotName := supporting.RandomTextString(20)
	stateStorageFile := fmt.Sprintf("/tmp/%s", supporting.RandomTextString(10))

	var tableName string
	pts.RunTest(
		func(context testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			if err := context.PauseReplicator(); err != nil {
				return err
			}

			// Break publication by dropping random chunks
			chunksToDrop := make([]string, 0)
			publishedChunkList := supporting.MapMapper(
				publishedChunks,
				func(_ string, element systemcatalog.SystemEntity) string {
					return element.CanonicalName()
				},
			)
			publishedChunkList = supporting.Filter(publishedChunkList, func(item string) bool {
				return item != "\"_timescaledb_catalog\".\"chunk\"" &&
					item != "\"_timescaledb_catalog\".\"hypertable\""
			})

			for i := 0; i < 10; i++ {
				idx := supporting.RandomNumber(0, len(publishedChunkList))
				chunksToDrop = append(chunksToDrop, publishedChunkList[idx])
			}

			if err := context.PrivilegedContext(func(context testrunner.PrivilegedContext) error {
				_, err := context.Exec(stdctx.Background(),
					fmt.Sprintf(
						sqlDropChunksFromPublication, publicationName, strings.Join(chunksToDrop, ","),
					),
				)
				return err
			}); err != nil {
				return err
			}

			// Create a new chunk
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-26 00:00:00'::TIMESTAMPTZ, '2023-03-26 00:00:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			// Empty sink and reset the waiter
			testSink.Clear()
			waiter.Reset()

			if err := context.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			existingChunks, publishedChunks, err = readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			return nil
		},

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour,
				inttest.NewColumn("ts", "timestamptz", false, false, nil),
				inttest.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
				config.PostgreSQL.Publication.AutoDrop = supporting.AddrOf(false)

				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.AutoDrop = supporting.AddrOf(false)

				config.StateStorage.Type = spiconfig.FileStorage
				config.StateStorage.FileStorage = spiconfig.FileStorageConfig{
					Path: stateStorageFile,
				}
			})
			return nil
		}),

		testrunner.WithTearDown(func(context testrunner.Context) error {
			// If error happens, we don't care
			os.Remove(stateStorageFile)
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Fixing_Broken_Publications_Without_State_Storage() {
	waiter := supporting.NewWaiterWithTimeout(time.Second * 60)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			waiter.Signal()
		}),
	)

	publicationName := supporting.RandomTextString(10)
	replicationSlotName := supporting.RandomTextString(20)
	stateStorageFile := fmt.Sprintf("/tmp/%s", supporting.RandomTextString(10))

	var tableName string
	pts.RunTest(
		func(context testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			if err := context.PauseReplicator(); err != nil {
				return err
			}

			// Break publication by dropping random chunks
			chunksToDrop := make([]string, 0)
			publishedChunkList := supporting.MapMapper(
				publishedChunks,
				func(_ string, element systemcatalog.SystemEntity) string {
					return element.CanonicalName()
				},
			)
			publishedChunkList = supporting.Filter(publishedChunkList, func(item string) bool {
				return item != "\"_timescaledb_catalog\".\"chunk\"" &&
					item != "\"_timescaledb_catalog\".\"hypertable\""
			})

			for i := 0; i < 10; i++ {
				idx := supporting.RandomNumber(0, len(publishedChunkList))
				chunksToDrop = append(chunksToDrop, publishedChunkList[idx])
			}

			if err := context.PrivilegedContext(func(context testrunner.PrivilegedContext) error {
				_, err := context.Exec(stdctx.Background(),
					fmt.Sprintf(
						sqlDropChunksFromPublication, publicationName, strings.Join(chunksToDrop, ","),
					),
				)
				return err
			}); err != nil {
				return err
			}

			// Create a new chunk
			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-26 00:00:00'::TIMESTAMPTZ, '2023-03-26 00:00:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			// Empty sink and reset the waiter
			testSink.Clear()
			waiter.Reset()

			if err := context.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			existingChunks, publishedChunks, err = readAllAndPublishedChunks(context, tableName, publicationName)
			if err != nil {
				return err
			}

			if len(existingChunks) != len(publishedChunks)-2 {
				pts.T().Errorf(
					"Number of existing and published chunks doesn't match: %d!=%d",
					len(existingChunks),
					len(publishedChunks)-2,
				)
			}

			for key := range existingChunks {
				if _, present := publishedChunks[key]; !present {
					pts.T().Errorf("Chunk %s exists but isn't published", key)
				}
			}

			return nil
		},

		testrunner.WithSetup(func(context testrunner.SetupContext) error {
			_, tn, err := context.CreateHypertable("ts", time.Hour,
				inttest.NewColumn("ts", "timestamptz", false, false, nil),
				inttest.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := context.Exec(stdctx.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			context.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			context.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
				config.PostgreSQL.Publication.AutoDrop = supporting.AddrOf(false)

				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.AutoDrop = supporting.AddrOf(false)
			})
			return nil
		}),

		testrunner.WithTearDown(func(context testrunner.Context) error {
			// If error happens, we don't care
			os.Remove(stateStorageFile)
			return nil
		}),
	)
}

func readAllAndPublishedChunks(context testrunner.Context, tableName, publicationName string) (
	existingChunks map[string]systemcatalog.SystemEntity,
	publishedChunks map[string]systemcatalog.SystemEntity,
	err error,
) {

	existingChunks = make(map[string]systemcatalog.SystemEntity)
	publishedChunks = make(map[string]systemcatalog.SystemEntity)

	if rows, err := context.Query(
		stdctx.Background(), sqlReadAllChunksForHypertable, tableName,
	); err != nil {
		return nil, nil, err
	} else {
		for rows.Next() {
			var schemaName, tableName string
			if err := rows.Scan(&schemaName, &tableName); err != nil {
				return nil, nil, err
			}
			systemEntity := systemcatalog.NewSystemEntity(schemaName, tableName)
			existingChunks[systemEntity.CanonicalName()] = systemEntity
		}
	}

	if rows, err := context.Query(
		stdctx.Background(), sqlReadPublishedChunks, publicationName,
	); err != nil {
		return nil, nil, err
	} else {
		for rows.Next() {
			var schemaName, tableName string
			if err := rows.Scan(&schemaName, &tableName); err != nil {
				return nil, nil, err
			}
			systemEntity := systemcatalog.NewSystemEntity(schemaName, tableName)
			publishedChunks[systemEntity.CanonicalName()] = systemEntity
		}
	}
	return existingChunks, publishedChunks, nil
}
