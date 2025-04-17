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
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/samber/lo"
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

func TestPublicationTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(PublicationTestSuite))
}

func (pts *PublicationTestSuite) Test_Preexisting_Chunks_Added_To_Publication() {
	testSink := testsupport.NewEventCollectorSink()
	publicationName := lo.RandomString(10, lo.LowerCaseLettersCharset)

	var tableName string
	pts.RunTest(
		func(ctx testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(ctx, tableName, publicationName)
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

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
			})
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Reloading_From_Known_Chunks() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			waiter.Signal()
		}),
	)

	publicationName := lo.RandomString(10, lo.LowerCaseLettersCharset)
	replicationSlotName := lo.RandomString(20, lo.LowerCaseLettersCharset)
	stateStorageFile := fmt.Sprintf("/tmp/%s", lo.RandomString(10, lo.LowerCaseLettersCharset))

	var tableName string
	pts.RunTest(
		func(ctx testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(ctx, tableName, publicationName)
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

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}

			// Create a new chunk
			if _, err := ctx.Exec(context.Background(),
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

			if err := ctx.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			existingChunks, publishedChunks, err = readAllAndPublishedChunks(ctx, tableName, publicationName)
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

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
				config.PostgreSQL.Publication.AutoDrop = lo.ToPtr(false)

				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.AutoDrop = lo.ToPtr(false)

				config.StateStorage.Type = spiconfig.FileStorage
				config.StateStorage.FileStorage = spiconfig.FileStorageConfig{
					Path: stateStorageFile,
				}
			})
			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			// If error happens, we don't care
			os.Remove(stateStorageFile)
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Fixing_Broken_Publications_With_State_Storage() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 20)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			waiter.Signal()
		}),
	)

	publicationName := lo.RandomString(10, lo.LowerCaseLettersCharset)
	replicationSlotName := lo.RandomString(20, lo.LowerCaseLettersCharset)
	stateStorageFile := fmt.Sprintf("/tmp/%s", lo.RandomString(10, lo.LowerCaseLettersCharset))

	var tableName string
	pts.RunTest(
		func(ctx testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(ctx, tableName, publicationName)
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

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}

			// Break publication by dropping random chunks
			chunksToDrop := make([]string, 0)
			publishedChunkList := lo.MapToSlice(
				publishedChunks,
				func(_ string, element systemcatalog.SystemEntity) string {
					return element.CanonicalName()
				},
			)
			publishedChunkList = lo.Filter(publishedChunkList, func(item string, _ int) bool {
				return item != "\"_timescaledb_catalog\".\"chunk\"" &&
					item != "\"_timescaledb_catalog\".\"hypertable\""
			})

			for i := 0; i < 10; i++ {
				idx := testsupport.RandomNumber(0, len(publishedChunkList))
				chunksToDrop = append(chunksToDrop, publishedChunkList[idx])
			}

			if err := ctx.PrivilegedContext(func(ctx testrunner.PrivilegedContext) error {
				_, err := ctx.Exec(context.Background(),
					fmt.Sprintf(
						sqlDropChunksFromPublication, publicationName, strings.Join(chunksToDrop, ","),
					),
				)
				return err
			}); err != nil {
				return err
			}

			// Create a new chunk
			if _, err := ctx.Exec(context.Background(),
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

			if err := ctx.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			existingChunks, publishedChunks, err = readAllAndPublishedChunks(ctx, tableName, publicationName)
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

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
				config.PostgreSQL.Publication.AutoDrop = lo.ToPtr(false)

				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.AutoDrop = lo.ToPtr(false)

				config.StateStorage.Type = spiconfig.FileStorage
				config.StateStorage.FileStorage = spiconfig.FileStorageConfig{
					Path: stateStorageFile,
				}
			})
			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			// If error happens, we don't care
			os.Remove(stateStorageFile)
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Fixing_Broken_Publications_Without_State_Storage() {
	waiter := waiting.NewWaiterWithTimeout(time.Second * 60)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			waiter.Signal()
		}),
	)

	publicationName := lo.RandomString(10, lo.LowerCaseLettersCharset)
	replicationSlotName := lo.RandomString(20, lo.LowerCaseLettersCharset)
	stateStorageFile := fmt.Sprintf("/tmp/%s", lo.RandomString(10, lo.LowerCaseLettersCharset))

	var tableName string
	pts.RunTest(
		func(ctx testrunner.Context) error {
			existingChunks, publishedChunks, err := readAllAndPublishedChunks(ctx, tableName, publicationName)
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

			if err := ctx.PauseReplicator(); err != nil {
				return err
			}

			// Break publication by dropping random chunks
			chunksToDrop := make([]string, 0)
			publishedChunkList := lo.MapToSlice(
				publishedChunks,
				func(_ string, element systemcatalog.SystemEntity) string {
					return element.CanonicalName()
				},
			)
			publishedChunkList = lo.Filter(publishedChunkList, func(item string, _ int) bool {
				return item != "\"_timescaledb_catalog\".\"chunk\"" &&
					item != "\"_timescaledb_catalog\".\"hypertable\""
			})

			for i := 0; i < 10; i++ {
				idx := testsupport.RandomNumber(0, len(publishedChunkList))
				chunksToDrop = append(chunksToDrop, publishedChunkList[idx])
			}

			if err := ctx.PrivilegedContext(func(ctx testrunner.PrivilegedContext) error {
				_, err := ctx.Exec(context.Background(),
					fmt.Sprintf(
						sqlDropChunksFromPublication, publicationName, strings.Join(chunksToDrop, ","),
					),
				)
				return err
			}); err != nil {
				return err
			}

			// Create a new chunk
			if _, err := ctx.Exec(context.Background(),
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

			if err := ctx.ResumeReplicator(); err != nil {
				return err
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			existingChunks, publishedChunks, err = readAllAndPublishedChunks(ctx, tableName, publicationName)
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

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
				config.PostgreSQL.Publication.AutoDrop = lo.ToPtr(false)

				config.PostgreSQL.ReplicationSlot.Name = replicationSlotName
				config.PostgreSQL.ReplicationSlot.AutoDrop = lo.ToPtr(false)
			})
			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			// If error happens, we don't care
			os.Remove(stateStorageFile)
			return nil
		}),
	)
}

func (pts *PublicationTestSuite) Test_Dropped_Chunks_Should_Be_Ignored() {
	testSink := testsupport.NewEventCollectorSink()
	publicationName := lo.RandomString(10, lo.LowerCaseLettersCharset)

	var tableName string
	pts.RunTest(
		func(ctx testrunner.Context) error {
			return nil
		},

		testrunner.WithSetup(func(ctx testrunner.SetupContext) error {
			_, tn, err := ctx.CreateHypertable("ts", time.Hour,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 23:59:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					tableName,
				),
			); err != nil {
				return err
			}

			var insertDroppedChunkQuery string

			if ctx.TimescaleVersion().Compare(21200) >= 0 {
				insertDroppedChunkQuery = "INSERT INTO _timescaledb_catalog.chunk(hypertable_id, schema_name, table_name, compressed_chunk_id, dropped, status, osm_chunk, creation_time) VALUES (1, '_timescaledb_internal', '_hyper_1_427_chunk', NULL, TRUE, 0, FALSE, NOW())"
			} else {
				insertDroppedChunkQuery = "INSERT INTO _timescaledb_catalog.chunk(hypertable_id, schema_name, table_name, compressed_chunk_id, dropped, status, osm_chunk) VALUES (1, '_timescaledb_internal', '_hyper_1_427_chunk', null, true, 0, false)"
			}

			if _, err := ctx.Exec(context.Background(), insertDroppedChunkQuery); err != nil {
				return err
			}

			ctx.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			ctx.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.PostgreSQL.Publication.Name = publicationName
			})
			return nil
		}),
	)
}

func readAllAndPublishedChunks(
	ctx testrunner.Context, tableName, publicationName string,
) (
	existingChunks map[string]systemcatalog.SystemEntity,
	publishedChunks map[string]systemcatalog.SystemEntity,
	err error,
) {

	existingChunks = make(map[string]systemcatalog.SystemEntity)
	publishedChunks = make(map[string]systemcatalog.SystemEntity)

	if rows, err := ctx.Query(
		context.Background(), sqlReadAllChunksForHypertable, tableName,
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

	if rows, err := ctx.Query(
		context.Background(), sqlReadPublishedChunks, publicationName,
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
