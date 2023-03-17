package replication

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"strings"
)

const dropPublication = "DROP PUBLICATION IF EXISTS %s"
const createPublication = "CREATE PUBLICATION %s FOR TABLE _timescaledb_catalog.chunk, _timescaledb_catalog.hypertable"
const addTableToPublication = "ALTER PUBLICATION %s ADD TABLE %s"

const outputPlugin = "pgoutput"

type replicationChannel struct {
	connConfig      *pgconn.Config
	publicationName string
	shutdownStart   chan bool
	shutdownEnd     chan bool
}

func newReplicationChannel(connConfig *pgx.ConnConfig, publicationName string) *replicationChannel {
	connConfig = connConfig.Copy()
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}
	connConfig.RuntimeParams["replication"] = "database"

	return &replicationChannel{
		connConfig:      &connConfig.Config,
		publicationName: publicationName,
		shutdownStart:   make(chan bool, 1),
		shutdownEnd:     make(chan bool, 1),
	}
}

func (rc *replicationChannel) stopReplicationChannel() {
	rc.shutdownStart <- true
	<-rc.shutdownEnd
}

func (rc *replicationChannel) startReplicationChannel(
	dispatcher *eventhandler.Dispatcher, initialChunkTables []string) error {

	replicationHandler := newReplicationHandler(dispatcher)
	connection, err := pgconn.ConnectConfig(context.Background(), rc.connConfig)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	if err := rc.executeQuery(connection, fmt.Sprintf(dropPublication, rc.publicationName)); err != nil {
		return errors.Wrap(err, 0)
	}
	if err := rc.executeQuery(connection, fmt.Sprintf(createPublication, rc.publicationName)); err != nil {
		return errors.Wrap(err, 0)
	}

	attachingQuery := fmt.Sprintf(addTableToPublication, rc.publicationName, strings.Join(initialChunkTables, ","))
	if err := rc.executeQuery(connection, attachingQuery); err != nil {
		return errors.Wrap(err, 0)
	}

	identification, err := pglogrepl.IdentifySystem(context.Background(), connection)
	if err != nil {
		return fmt.Errorf("system identification failed: %s", err)
	}
	logger.Println("SystemID:", identification.SystemID, "Timeline:", identification.Timeline, "XLogPos:",
		identification.XLogPos, "DBName:", identification.DBName)

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", rc.publicationName)}
	/*slot*/ _, err = pglogrepl.CreateReplicationSlot(context.Background(), connection, rc.publicationName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: true, SnapshotAction: "EXPORT_SNAPSHOT"})

	if err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %s", err)
	}
	logger.Println("Created temporary replication slot:", rc.publicationName)

	if err := pglogrepl.StartReplication(context.Background(), connection, rc.publicationName, identification.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}); err != nil {

		return fmt.Errorf("StartReplication failed: %s", err)
	}

	go func() {
		err := replicationHandler.startReplicationHandler(connection, identification)
		if err != nil {
			logger.Fatalf("Issue handling WAL stream: %s", err)
		}
		rc.shutdownStart <- true
	}()

	<-rc.shutdownStart
	replicationHandler.stopReplicationHandler()
	if _, err := pglogrepl.SendStandbyCopyDone(context.Background(), connection); err != nil {
		return errors.Wrap(err, 0)
	}
	if err := pglogrepl.DropReplicationSlot(context.Background(), connection, rc.publicationName, pglogrepl.DropReplicationSlotOptions{Wait: true}); err != nil {
		return errors.Wrap(err, 0)
	}
	if err := rc.executeQuery(connection, fmt.Sprintf(dropPublication, rc.publicationName)); err != nil {
		return errors.Wrap(err, 0)
	}
	rc.shutdownEnd <- true

	return nil
}

func (rc *replicationChannel) executeQuery(connection *pgconn.PgConn, query string) error {
	result := connection.Exec(context.Background(), query)
	_, err := result.ReadAll()
	return errors.Wrap(err, 0)
}
