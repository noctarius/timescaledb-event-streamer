package channels

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	repcontext "github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

const outputPlugin = "pgoutput"

type ReplicationChannel struct {
	connConfig             *pgconn.Config
	replicationContext     *repcontext.ReplicationContext
	createdPublication     bool
	createdReplicationSlot bool
	shutdownAwaiter        *supporting.ShutdownAwaiter
}

func NewReplicationChannel(connConfig *pgx.ConnConfig,
	replicationContext *repcontext.ReplicationContext) *ReplicationChannel {

	connConfig = connConfig.Copy()
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}
	connConfig.RuntimeParams["replication"] = "database"

	return &ReplicationChannel{
		connConfig:         &connConfig.Config,
		replicationContext: replicationContext,
		shutdownAwaiter:    supporting.NewShutdownAwaiter(),
	}
}

func (rc *ReplicationChannel) StopReplicationChannel() error {
	rc.shutdownAwaiter.SignalShutdown()
	return rc.shutdownAwaiter.AwaitDone()
}

func (rc *ReplicationChannel) StartReplicationChannel(
	replicationContext *repcontext.ReplicationContext, initialTables []systemcatalog.SystemEntity) error {

	replicationHandler := newReplicationHandler(replicationContext)
	connection, err := pgconn.ConnectConfig(context.Background(), rc.connConfig)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	if created, err := rc.replicationContext.CreatePublication(); created {
		rc.createdPublication = true
	} else if err != nil {
		return errors.Wrap(err, 0)
	}

	if len(initialTables) > 0 {
		if err := rc.replicationContext.AttachTablesToPublication(initialTables...); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// Retrieve system identifiers for replication
	identification, err := pglogrepl.IdentifySystem(context.Background(), connection)
	if err != nil {
		return fmt.Errorf("system identification failed: %s", err)
	}
	logger.Println("SystemID:", identification.SystemID, "Timeline:", identification.Timeline, "XLogPos:",
		identification.XLogPos, "DBName:", identification.DBName)

	// Build output plugin parameters
	pluginArguments := []string{
		fmt.Sprintf("publication_names '%s'", rc.replicationContext.PublicationName()),
	}
	if replicationContext.IsPG14GE() {
		pluginArguments = append(
			pluginArguments,
			"proto_version '2'",
			"messages 'true'",
			"binary 'true'",
		)
	} else {
		pluginArguments = append(
			pluginArguments,
			"proto_version '1'",
		)
	}

	if err := rc.ensureReplicationSlot(connection); err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %s", err)
	}
	logger.Println("Created temporary replication slot:", rc.replicationContext.PublicationName())

	if err := rc.startReplication(connection, identification, pluginArguments); err != nil {
		return fmt.Errorf("StartReplication failed: %s", err)
	}

	go func() {
		err := replicationHandler.startReplicationHandler(connection, identification)
		if err != nil {
			logger.Fatalf("Issue handling WAL stream: %s", err)
		}
		rc.shutdownAwaiter.SignalShutdown()
	}()

	go func() {
		if err := rc.shutdownAwaiter.AwaitShutdown(); err != nil {
			logger.Errorf("shutdown failed: %+v", err)
		}
		if err := replicationHandler.stopReplicationHandler(); err != nil {
			logger.Errorf("shutdown failed (stop replication handler): %+v", err)
		}
		if err := rc.stopReplication(connection); err != nil {
			logger.Errorf("shutdown failed (send copy done): %+v", err)
		}
		if err := rc.dropReplicationSlotIfNecessary(connection); err != nil {
			logger.Errorf("shutdown failed (drop replication slot): %+v", err)
		}
		if rc.createdPublication {
			if err := rc.replicationContext.DropPublication(); err != nil {
				logger.Errorf("shutdown failed (drop publication): %+v", err)
			}
		}
		if err := connection.Close(context.Background()); err != nil {
			logger.Warnf("failed to close replication connection: %+v", err)
		}
		rc.shutdownAwaiter.SignalDone()
	}()

	return nil
}

func (rc *ReplicationChannel) ensureReplicationSlot(connection *pgconn.PgConn) error {
	_ /*slot*/, err := pglogrepl.CreateReplicationSlot(
		context.Background(),
		connection,
		rc.replicationContext.PublicationName(),
		outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true,
			SnapshotAction: "EXPORT_SNAPSHOT",
		},
	)
	rc.createdReplicationSlot = true
	return err
}

func (rc *ReplicationChannel) dropReplicationSlotIfNecessary(connection *pgconn.PgConn) error {
	if !rc.createdReplicationSlot {
		return nil
	}
	return pglogrepl.DropReplicationSlot(
		context.Background(),
		connection,
		rc.replicationContext.PublicationName(),
		pglogrepl.DropReplicationSlotOptions{
			Wait: true,
		},
	)
}

func (rc *ReplicationChannel) startReplication(connection *pgconn.PgConn,
	identification pglogrepl.IdentifySystemResult, pluginArguments []string) error {

	return pglogrepl.StartReplication(
		context.Background(),
		connection,
		rc.replicationContext.PublicationName(),
		identification.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		},
	)
}

func (rc *ReplicationChannel) stopReplication(connection *pgconn.PgConn) error {
	_, err := pglogrepl.SendStandbyCopyDone(context.Background(), connection)
	if e, ok := err.(*pgconn.PgError); ok {
		if e.Code == "XX000" {
			return nil
		}
	}
	return err
}
