package replicationchannel

import (
	"fmt"
	"github.com/go-errors/errors"
	repcontext "github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

// ReplicationChannel represents the database connection and handler loop
// for the logical replication decoding subscriber.
type ReplicationChannel struct {
	replicationContext *repcontext.ReplicationContext
	createdPublication bool
	shutdownAwaiter    *supporting.ShutdownAwaiter
	logger             *logging.Logger
}

// NewReplicationChannel instantiates a new instance of the ReplicationChannel.
func NewReplicationChannel(replicationContext *repcontext.ReplicationContext) (*ReplicationChannel, error) {
	logger, err := logging.NewLogger("ReplicationChannel")
	if err != nil {
		return nil, err
	}

	return &ReplicationChannel{
		replicationContext: replicationContext,
		shutdownAwaiter:    supporting.NewShutdownAwaiter(),
		logger:             logger,
	}, nil
}

// StopReplicationChannel initiates a clean shutdown of the replication channel
// and logical replication handler loop. This call will block until the loop is
// cleanly shut down.
func (rc *ReplicationChannel) StopReplicationChannel() error {
	rc.shutdownAwaiter.SignalShutdown()
	return rc.shutdownAwaiter.AwaitDone()
}

// StartReplicationChannel starts the replication channel, as well as initializes
// and starts the logical replication handler loop.
func (rc *ReplicationChannel) StartReplicationChannel(
	replicationContext *repcontext.ReplicationContext, initialTables []systemcatalog.SystemEntity) error {

	replicationHandler, err := newReplicationHandler(replicationContext)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	replicationConnection, err := replicationContext.NewReplicationConnection()
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

	if slotName, _, err := replicationConnection.CreateReplicationSlot(); err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %s", err)
	} else {
		rc.logger.Println("Created temporary replication slot:", slotName)
	}

	if err := replicationConnection.StartReplication(pluginArguments); err != nil {
		return fmt.Errorf("StartReplication failed: %s", err)
	}

	go func() {
		err := replicationHandler.startReplicationHandler(replicationConnection)
		if err != nil {
			rc.logger.Fatalf("Issue handling WAL stream: %s", err)
		}
		rc.shutdownAwaiter.SignalShutdown()
	}()

	go func() {
		if err := rc.shutdownAwaiter.AwaitShutdown(); err != nil {
			rc.logger.Errorf("shutdown failed: %+v", err)
		}
		if err := replicationHandler.stopReplicationHandler(); err != nil {
			rc.logger.Errorf("shutdown failed (stop replication handler): %+v", err)
		}
		if err := replicationConnection.StopReplication(); err != nil {
			rc.logger.Errorf("shutdown failed (send copy done): %+v", err)
		}
		if err := replicationConnection.DropReplicationSlot(); err != nil {
			rc.logger.Errorf("shutdown failed (drop replication slot): %+v", err)
		}
		if rc.createdPublication {
			if err := rc.replicationContext.DropPublication(); err != nil {
				rc.logger.Errorf("shutdown failed (drop publication): %+v", err)
			}
		}
		if err := replicationConnection.Close(); err != nil {
			rc.logger.Warnf("failed to close replication connection: %+v", err)
		}
		rc.shutdownAwaiter.SignalDone()
	}()

	return nil
}
