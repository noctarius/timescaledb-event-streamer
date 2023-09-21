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

package replicationchannel

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationconnection"
	"github.com/noctarius/timescaledb-event-streamer/internal/stats"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/publication"
	"github.com/noctarius/timescaledb-event-streamer/spi/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"sync/atomic"
)

// ReplicationChannel represents the database connection and handler loop
// for the logical replication decoding subscriber.
type ReplicationChannel struct {
	replicationContext replicationcontext.ReplicationContext
	publicationManager publication.PublicationManager
	typeManager        pgtypes.TypeManager
	taskManager        task.TaskManager
	createdPublication bool
	shutdownAwaiter    *waiting.ShutdownAwaiter
	statsReporter      *stats.Reporter
	logger             *logging.Logger
	shutdownRequested  atomic.Bool
}

// NewReplicationChannel instantiates a new instance of the ReplicationChannel.
func NewReplicationChannel(
	replicationContext replicationcontext.ReplicationContext, typeManager pgtypes.TypeManager,
	taskManager task.TaskManager, publicationManager publication.PublicationManager,
	statsService *stats.Service,
) (*ReplicationChannel, error) {

	logger, err := logging.NewLogger("ReplicationChannel")
	if err != nil {
		return nil, err
	}

	return &ReplicationChannel{
		replicationContext: replicationContext,
		publicationManager: publicationManager,
		typeManager:        typeManager,
		taskManager:        taskManager,
		shutdownAwaiter:    waiting.NewShutdownAwaiter(),
		logger:             logger,
		statsReporter:      statsService.NewReporter("streamer_replicationchannel"),
	}, nil
}

// StopReplicationChannel initiates a clean shutdown of the replication channel
// and logical replication handler loop. This call will block until the loop is
// cleanly shut down.
func (rc *ReplicationChannel) StopReplicationChannel() error {
	rc.shutdownRequested.Store(true)
	rc.shutdownAwaiter.SignalShutdown()
	return rc.shutdownAwaiter.AwaitDone()
}

// StartReplicationChannel starts the replication channel, as well as initializes
// and starts the logical replication handler loop.
func (rc *ReplicationChannel) StartReplicationChannel(
	initialTables []systemcatalog.SystemEntity,
) error {

	handler, err := newReplicationHandler(rc.replicationContext, rc.typeManager, rc.taskManager, rc.statsReporter)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	replicationConnection, err := replicationconnection.NewReplicationConnection(rc.replicationContext)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	if found, err := rc.publicationManager.ExistsPublication(); err != nil {
		return errors.Wrap(err, 0)
	} else if !found {
		if !rc.publicationManager.PublicationCreate() {
			return errors.Errorf("Publication missing but wasn't asked to create it either")
		}
		if created, err := rc.publicationManager.CreatePublication(); created {
			rc.createdPublication = true
		} else if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// Build output plugin parameters
	pluginArguments := []string{
		fmt.Sprintf("publication_names '%s'", rc.publicationManager.PublicationName()),
	}
	if rc.replicationContext.IsPG14GE() {
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
	} else {
		rc.logger.Println("Reused replication slot:", slotName)
	}

	// If we have missing chunks in the publication, we need to add them now. This list is either
	// generated from the list of known, previously existing, chunks (if the state storage contains
	// that information) or from the database, which provides all chunks at the point in time. This
	// may include chunks whose creating may be included in the WAL log, but there's nothing we can
	// do about it 🤷
	if len(initialTables) > 0 {
		if err := rc.publicationManager.AttachTablesToPublication(initialTables...); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	offset, err := rc.replicationContext.Offset()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	stopReplication := func() {
		rc.shutdownRequested.Store(true)
	}

	startReplication := func() error {
		if rc.shutdownRequested.Load() {
			// If already shutting down, ignore the request
			return nil
		}

		restartLSN, err := replicationConnection.StartReplication(pluginArguments)
		if err != nil {
			if rc.shutdownRequested.Load() {
				// If we tried to start replication before the shutdown was initiated,
				// we totally expect that to fail, and we can just ignore the error
				return nil
			}
			return errors.Errorf("StartReplication failed: %s", err)
		}

		go func() {
			err := handler.startReplicationHandler(replicationConnection, restartLSN)
			if err != nil {
				rc.logger.Fatalf("Issue handling WAL stream: %s", err)
			}
			rc.shutdownAwaiter.SignalShutdown()
		}()

		stopReplication = func() {
			if err := handler.stopReplicationHandler(); err != nil {
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
		rc.taskManager.RegisterReplicationEventHandler(
			&snapshottingEventHandler{
				startReplication: startReplication,
			},
		)

		// Kick of the actual snapshotting
		if err := rc.taskManager.EnqueueTask(func(notificator task.Notificator) {
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
		// Mark the current replication channel as shutting down
		rc.shutdownRequested.Store(true)

		if err := rc.shutdownAwaiter.AwaitShutdown(); err != nil {
			rc.logger.Errorf("shutdown failed: %+v", err)
		}

		// Stop potentially started replication before going on
		stopReplication()

		if err := replicationConnection.DropReplicationSlot(); err != nil {
			rc.logger.Errorf("shutdown failed (drop replication slot): %+v", err)
		}
		if rc.createdPublication && rc.publicationManager.PublicationAutoDrop() {
			if err := rc.publicationManager.DropPublication(); err != nil {
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

func (s *snapshottingEventHandler) OnRelationEvent(
	_ pgtypes.XLogData, _ *pgtypes.RelationMessage,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotStartedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotFinishedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk, _ pgtypes.LSN,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnTableSnapshotStartedEvent(
	_ string, _ systemcatalog.BaseTable,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnTableSnapshotFinishedEvent(
	_ string, _ systemcatalog.BaseTable, _ pgtypes.LSN,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnSnapshottingStartedEvent(
	_ string,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnSnapshottingFinishedEvent() error {
	return s.startReplication()
}
