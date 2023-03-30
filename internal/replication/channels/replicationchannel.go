package channels

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/pg/decoding"
	"strings"
)

const dropPublication = "DROP PUBLICATION IF EXISTS %s"
const checkExistingPublication = "SELECT true FROM pg_publication WHERE pubname = '%s'"
const createPublication = "SELECT create_timescaledb_catalog_publication('%s', '%s');"

const outputPlugin = "pgoutput"

type replicationChannel struct {
	connConfig         *pgconn.Config
	publicationName    string
	createdPublication bool
	shutdownStart      chan bool
	shutdownEnd        chan bool
}

func NewReplicationChannel(connConfig *pgx.ConnConfig, publicationName string) ReplicationChannel {
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

func (rc *replicationChannel) StopReplicationChannel() {
	rc.shutdownStart <- true
	<-rc.shutdownEnd
}

func (rc *replicationChannel) StartReplicationChannel(
	dispatcher *eventhandler.Dispatcher, initialChunkTables []string) error {

	replicationHandler := newReplicationHandler(dispatcher)
	connection, err := pgconn.ConnectConfig(context.Background(), rc.connConfig)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	if created, err := rc.createPublication(connection); created {
		rc.createdPublication = true
	} else if err != nil {
		return errors.Wrap(err, 0)
	}

	if len(initialChunkTables) > 0 {
		attachingQuery := fmt.Sprintf(addTableToPublication, rc.publicationName, strings.Join(initialChunkTables, ","))
		if err := rc.executeQuery(connection, attachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
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

	go func() {
		<-rc.shutdownStart
		replicationHandler.stopReplicationHandler()
		if _, err := pglogrepl.SendStandbyCopyDone(context.Background(), connection); err != nil {
			logger.Fatalf("shutdown failed: %+v", err)
		}
		if err := pglogrepl.DropReplicationSlot(context.Background(), connection, rc.publicationName, pglogrepl.DropReplicationSlotOptions{Wait: true}); err != nil {
			logger.Fatalf("shutdown failed: %+v", err)
		}
		if err := rc.executeQuery(connection, fmt.Sprintf(dropPublication, rc.publicationName)); err != nil {
			logger.Fatalf("shutdown failed: %+v", err)
		}
		rc.shutdownEnd <- true
	}()

	return nil
}

func (rc *replicationChannel) executeQuery(connection *pgconn.PgConn, query string) error {
	result := connection.Exec(context.Background(), query)
	_, err := result.ReadAll()
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (rc *replicationChannel) publicationExists(connection *pgconn.PgConn) bool {
	reader := connection.Exec(context.Background(), fmt.Sprintf(checkExistingPublication, rc.publicationName))
	if v, err := reader.ReadAll(); err != nil {
		return false
	} else {
		return len(v) > 0
	}
}

func (rc *replicationChannel) createPublication(connection *pgconn.PgConn) (bool, error) {
	reader := connection.Exec(context.Background(),
		fmt.Sprintf(createPublication, rc.publicationName, rc.connConfig.User))

	defer reader.Close()
	for reader.NextResult() {
		fields := reader.ResultReader().FieldDescriptions()
		if reader.ResultReader().NextRow() {
			rawRow := reader.ResultReader().Values()
			for i := 0; i < len(fields); i++ {
				val, err := decoding.DecodeValue(fields[i], rawRow[i])
				if err != nil {
					return false, err
				}
				if v, ok := val.(bool); ok {
					return v, nil
				}
			}
		}
	}
	return false, nil
}

func (rc *replicationChannel) attachChunkTables(connection *pgconn.PgConn, initialChunkTables []string) error {
	chunkTableList := strings.Join(initialChunkTables, ",")
	attachingQuery := fmt.Sprintf(addTableToPublication, rc.publicationName, chunkTableList)
	if err := rc.executeQuery(connection, attachingQuery); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}
