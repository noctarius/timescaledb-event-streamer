package integration

import (
	stdctx "context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	inttest "github.com/noctarius/timescaledb-event-streamer/internal/testing"
	"github.com/noctarius/timescaledb-event-streamer/internal/testing/testrunner"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

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
			existingChunks := make(map[string]bool)
			publishedChunks := make(map[string]bool)
			if rows, err := context.Query(stdctx.Background(),
				"SELECT c.table_name FROM _timescaledb_catalog.chunk c LEFT JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id WHERE h.table_name = $1",
				tableName,
			); err != nil {
				return err
			} else {
				for rows.Next() {
					var chunk string
					if err := rows.Scan(&chunk); err != nil {
						return err
					}
					existingChunks[chunk] = true
				}
			}

			if rows, err := context.Query(stdctx.Background(),
				"SELECT c.relname FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid LEFT JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid WHERE p.pubname = $1",
				publicationName,
			); err != nil {
				return err
			} else {
				for rows.Next() {
					var chunk string
					if err := rows.Scan(&chunk); err != nil {
						return err
					}
					publishedChunks[chunk] = true
				}
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
				systemcatalog.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, nil),
				systemcatalog.NewColumn("val", pgtype.Int4OID, "integer", false, nil),
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
