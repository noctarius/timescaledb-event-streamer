package replicationchannel

import (
	"fmt"
	"github.com/go-errors/errors"
	repcontext "github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
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

	if found, err := rc.replicationContext.ExistsPublication(); err != nil {
		return errors.Wrap(err, 0)
	} else if !found {
		if !rc.replicationContext.PublicationCreate() {
			return errors.Errorf("Publication missing but wasn't asked to create it either")
		}
		if created, err := rc.replicationContext.CreatePublication(); created {
			rc.createdPublication = true
		} else if err != nil {
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

	slotName, snapshotName, createdReplicationSlot, err := replicationConnection.CreateReplicationSlot()
	if err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %s", err)
	} else if createdReplicationSlot {
		rc.logger.Println("Created replication slot:", slotName)

		// If slot was newly created we immediately try to add as many chunks to the publication
		// as possible, otherwise we wait for the catalog handler to do it one by one since we may
		// have to snapshot or replay some of them which were created while we were gone.
		if len(initialTables) > 0 {
			if err := rc.replicationContext.AttachTablesToPublication(initialTables...); err != nil {
				return errors.Wrap(err, 0)
			}
		}
	} else {
		rc.logger.Println("Reused replication slot:", slotName)
	}

	offset, err := rc.replicationContext.Offset()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	stopReplication := func() {
	}

	startReplication := func() error {
		if err := replicationConnection.StartReplication(pluginArguments); err != nil {
			return errors.Errorf("StartReplication failed: %s", err)
		}

		go func() {
			err := replicationHandler.startReplicationHandler(replicationConnection)
			if err != nil {
				rc.logger.Fatalf("Issue handling WAL stream: %s", err)
			}
			rc.shutdownAwaiter.SignalShutdown()
		}()

		stopReplication = func() {
			if err := replicationHandler.stopReplicationHandler(); err != nil {
				rc.logger.Errorf("shutdown failed (stop replication handler): %+v", err)
			}
			if err := replicationConnection.StopReplication(); err != nil {
				rc.logger.Errorf("shutdown failed (send copy done): %+v", err)
			}
		}

		return nil
	}

	initialSnapshotMode := rc.replicationContext.InitialSnapshotMode()
	if initialSnapshotMode == config.Always {
		// We always want to do a full snapshot on startup. Do we need a snapshot name here?
		if !createdReplicationSlot {
			return errors.Errorf("Snapshot mode 'always' must create a replication slot!")
		}
	} else if initialSnapshotMode == config.InitialOnly &&
		(offset == nil || (offset.Snapshot && offset.SnapshotName != nil)) {

		// We need an initial snapshot and we either haven't started any or need
		// to resume a previously started one.

		// Let's do some sanity checking and setup
		if offset != nil && offset.Snapshot {
			if createdReplicationSlot {
				return errors.Errorf(
					"Snapshot mode 'initial_only' found an existing " +
						"offset state with a newly created replication slot!",
				)
			}

			if offset.SnapshotName != nil {
				snapshotName = *offset.SnapshotName
			}
		}
	} else {
		snapshotName = ""
	}

	// Start snapshotting
	if snapshotName != "" {
		rc.replicationContext.RegisterReplicationEventHandler(
			&snapshottingEventHandler{
				startReplication: startReplication,
			},
		)

		// Kick of the actual snapshotting
		if err := rc.replicationContext.EnqueueTask(func(notificator repcontext.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnSnapshottingStartedEvent(snapshotName)
			})
		}); err != nil {
			return err
		}
	} else {
		// No snapshotting necessary, start replicator immediately
		if err := startReplication(); err != nil {
			return err
		}
	}

	go func() {
		if err := rc.shutdownAwaiter.AwaitShutdown(); err != nil {
			rc.logger.Errorf("shutdown failed: %+v", err)
		}

		// Stop potentially started replication before going on
		stopReplication()

		if err := replicationConnection.DropReplicationSlot(); err != nil {
			rc.logger.Errorf("shutdown failed (drop replication slot): %+v", err)
		}
		if rc.createdPublication && rc.replicationContext.PublicationAutoDrop() {
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

type snapshottingEventHandler struct {
	startReplication func() error
}

func (s *snapshottingEventHandler) OnRelationEvent(_ pgtypes.XLogData, _ *pgtypes.RelationMessage) error {
	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotStartedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotFinishedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk, _ pgtypes.LSN) error {

	return nil
}

func (s *snapshottingEventHandler) OnHypertableSnapshotStartedEvent(_ string, _ *systemcatalog.Hypertable) error {
	return nil
}

func (s *snapshottingEventHandler) OnHypertableSnapshotFinishedEvent(_ string, _ *systemcatalog.Hypertable) error {
	return nil
}

func (s *snapshottingEventHandler) OnSnapshottingStartedEvent(_ string) error {
	return nil
}

func (s *snapshottingEventHandler) OnSnapshottingFinishedEvent() error {
	return s.startReplication()
}
