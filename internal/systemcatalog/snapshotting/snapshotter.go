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

package snapshotting

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/publication"
	"github.com/noctarius/timescaledb-event-streamer/spi/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
	"hash/fnv"
	"time"
)

type SnapshotTask struct {
	Hypertable        *systemcatalog.Hypertable
	Chunk             *systemcatalog.Chunk
	Xld               *pgtypes.XLogData
	SnapshotName      *string
	nextSnapshotFetch bool
}

type Snapshotter struct {
	partitionCount     uint64
	replicationContext replicationcontext.ReplicationContext
	taskManager        task.TaskManager
	typeManager        pgtypes.TypeManager
	publicationManager publication.PublicationManager
	snapshotQueues     []chan SnapshotTask
	shutdownAwaiter    *waiting.MultiShutdownAwaiter
	logger             *logging.Logger
}

func NewSnapshotterFromConfig(
	c *config.Config, replicationContext replicationcontext.ReplicationContext,
	taskManager task.TaskManager, publicationManager publication.PublicationManager,
	typeManager pgtypes.TypeManager,
) (*Snapshotter, error) {

	parallelism := config.GetOrDefault(c, config.PropertySnapshotterParallelism, uint8(5))
	return NewSnapshotter(parallelism, replicationContext, taskManager, publicationManager, typeManager)
}

func NewSnapshotter(
	partitionCount uint8, replicationContext replicationcontext.ReplicationContext,
	taskManager task.TaskManager, publicationManager publication.PublicationManager,
	typeManager pgtypes.TypeManager,
) (*Snapshotter, error) {

	snapshotQueues := make([]chan SnapshotTask, partitionCount)
	for i := range snapshotQueues {
		snapshotQueues[i] = make(chan SnapshotTask, 128)
	}

	logger, err := logging.NewLogger("Snapshotter")
	if err != nil {
		return nil, err
	}

	return &Snapshotter{
		partitionCount:     uint64(partitionCount),
		replicationContext: replicationContext,
		taskManager:        taskManager,
		typeManager:        typeManager,
		publicationManager: publicationManager,
		snapshotQueues:     snapshotQueues,
		logger:             logger,
		shutdownAwaiter:    waiting.NewMultiShutdownAwaiter(uint(partitionCount)),
	}, nil
}

func (s *Snapshotter) EnqueueSnapshot(
	t SnapshotTask,
) error {

	enqueueSnapshotTask := func() {
		// Partition calculation
		hasher := fnv.New64a()
		if _, err := hasher.Write([]byte(t.Hypertable.CanonicalName())); err != nil {
			// If we cannot hash, the system will break. That means, we harshly kill the process.
			panic(err)
		}
		partition := int(hasher.Sum64() % s.partitionCount)

		// Enqueue the actual task
		s.snapshotQueues[partition] <- t
		if t.Chunk != nil {
			s.logger.Debugf(
				"Submitting snapshot request for chunk '%s' to partition %d",
				t.Chunk.TableName(), partition,
			)
		}
	}

	// Notify of snapshotting to save incoming events
	if t.Chunk != nil {
		err := s.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnChunkSnapshotStartedEvent(t.Hypertable, t.Chunk)
			})
			enqueueSnapshotTask()
		})
		if err != nil {
			return errors.Wrap(err, 0)
		}
	} else {
		enqueueSnapshotTask()
	}

	return nil
}

func (s *Snapshotter) StartSnapshotter() {
	for i := 0; i < int(s.partitionCount); i++ {
		go func(partition int) {
			for {
				select {
				case task := <-s.snapshotQueues[partition]:
					if err := s.snapshot(task); err != nil {
						s.logger.Fatalf("snapshotting of task '%+v' failed: %+v", task, err)
					}
				case <-s.shutdownAwaiter.AwaitShutdownChan(uint(partition)):
					goto shutdown
				case <-time.After(time.Second * 5):
					// timeout, keep running
				}
			}

		shutdown:
			s.shutdownAwaiter.SignalDone()
		}(i)
	}
}

func (s *Snapshotter) StopSnapshotter() {
	s.shutdownAwaiter.SignalShutdown()
	s.shutdownAwaiter.AwaitDone()
}

func (s *Snapshotter) snapshot(
	task SnapshotTask,
) error {

	if task.Chunk != nil {
		return s.snapshotChunk(task)
	}
	return s.snapshotHypertable(task)
}

func (s *Snapshotter) snapshotChunk(
	t SnapshotTask,
) error {

	alreadyPublished, err := s.publicationManager.ExistsTableInPublication(t.Chunk)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	if !alreadyPublished {
		if err := s.publicationManager.AttachTablesToPublication(t.Chunk); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	lsn, err := s.replicationContext.SnapshotChunkTable(
		s.typeManager.GetOrPlanRowDecoder, t.Chunk,
		func(lsn pgtypes.LSN, values map[string]any) error {
			return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
				callback := func(handler eventhandlers.HypertableReplicationEventHandler) error {
					return handler.OnReadEvent(lsn, t.Hypertable, t.Chunk, values)
				}
				if t.Xld != nil {
					callback = func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnInsertEvent(*t.Xld, t.Hypertable, t.Chunk, values)
					}
				}
				notificator.NotifyHypertableReplicationEventHandler(callback)
			})
		},
	)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return s.taskManager.EnqueueTaskAndWait(func(notificator task.Notificator) {
		notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
			return handler.OnChunkSnapshotFinishedEvent(t.Hypertable, t.Chunk, lsn)
		})
	})
}

func (s *Snapshotter) snapshotHypertable(
	t SnapshotTask,
) error {

	stateManager := s.replicationContext.StateStorageManager()

	// tableSnapshotState
	if err := stateManager.SnapshotContextTransaction(
		*t.SnapshotName, true,
		func(snapshotContext *watermark.SnapshotContext) error {
			hypertableWatermark, created := snapshotContext.GetOrCreateWatermark(t.Hypertable)

			// Initialize the watermark or update the high watermark after a restart
			if created || t.nextSnapshotFetch {
				highWatermark, err := s.replicationContext.ReadSnapshotHighWatermark(
					s.typeManager.GetOrPlanRowDecoder, t.Hypertable, *t.SnapshotName,
				)
				if err != nil {
					return errors.Wrap(err, 0)
				}

				hypertableWatermark.SetHighWatermark(highWatermark)
			}

			return nil
		},
	); err != nil {
		return errors.Wrap(err, 0)
	}

	// Kick off snapshot fetching
	if err := s.runSnapshotFetchBatch(t); err != nil {
		return errors.Wrap(err, 0)
	}

	return stateManager.SnapshotContextTransaction(
		*t.SnapshotName, false,
		func(snapshotContext *watermark.SnapshotContext) error {
			hypertableWatermark, present := snapshotContext.GetWatermark(t.Hypertable)
			if !present {
				return errors.Errorf(
					"illegal watermark state for hypertable '%s'", t.Hypertable.CanonicalName(),
				)
			}

			if hypertableWatermark.Complete() {
				return s.taskManager.EnqueueTaskAndWait(func(notificator task.Notificator) {
					notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
						return handler.OnHypertableSnapshotFinishedEvent(*t.SnapshotName, t.Hypertable)
					})
				})
			}

			return s.EnqueueSnapshot(SnapshotTask{
				Hypertable:        t.Hypertable,
				SnapshotName:      t.SnapshotName,
				nextSnapshotFetch: true,
			})
		},
	)
}

func (s *Snapshotter) runSnapshotFetchBatch(
	t SnapshotTask,
) error {

	return s.replicationContext.FetchHypertableSnapshotBatch(
		s.typeManager.GetOrPlanRowDecoder, t.Hypertable, *t.SnapshotName,
		func(lsn pgtypes.LSN, values map[string]any) error {
			return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnReadEvent(lsn, t.Hypertable, t.Chunk, values)
					},
				)
			})
		},
	)
}
