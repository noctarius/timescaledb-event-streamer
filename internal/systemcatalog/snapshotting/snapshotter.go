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
	"github.com/noctarius/timescaledb-event-streamer/internal/stats"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/publication"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
	"hash/fnv"
	"time"
)

type snapshotterStats struct {
	scheduler struct {
		partitionCount uint   `metric:"partitioncount" type:"gauge"`
		scheduled      uint64 `metric:"scheduled" type:"gauge"`
	} `metric:"scheduler"`
}

type snapshotterPartitionStats struct {
	snapshots struct {
		hypertables uint `metric:"hypertable" type:"gauge"`
		chunks      uint `metric:"chunks" type:"gauge"`
	} `metric:"snapshots"`
	records struct {
		total uint64 `metric:"total" type:"gauge"`
	} `metric:"records"`
}

type SnapshotTask struct {
	Hypertable        *systemcatalog.Hypertable
	Chunk             *systemcatalog.Chunk
	Xld               *pgtypes.XLogData
	SnapshotName      *string
	nextSnapshotFetch bool
}

type Snapshotter struct {
	partitionCount    uint64
	snapshotBatchSize int

	statsReporter       *stats.Reporter
	taskManager         task.TaskManager
	typeManager         pgtypes.TypeManager
	sideChannel         sidechannel.SideChannel
	stateStorageManager statestorage.Manager
	publicationManager  publication.PublicationManager
	snapshotQueues      []chan SnapshotTask
	shutdownAwaiter     *waiting.MultiShutdownAwaiter
	logger              *logging.Logger

	stats          snapshotterStats
	partitionStats []snapshotterPartitionStats
}

func NewSnapshotterFromConfig(
	c *config.Config, stateStorageManager statestorage.Manager, sideChannel sidechannel.SideChannel,
	taskManager task.TaskManager, publicationManager publication.PublicationManager,
	typeManager pgtypes.TypeManager, statsService *stats.Service,
) (*Snapshotter, error) {

	parallelism := config.GetOrDefault(c, config.PropertySnapshotterParallelism, uint8(5))
	snapshotBatchSize := config.GetOrDefault(c, config.PropertyPostgresqlSnapshotBatchsize, 1000)
	return NewSnapshotter(
		parallelism, snapshotBatchSize, stateStorageManager, sideChannel,
		taskManager, publicationManager, typeManager, statsService,
	)
}

func NewSnapshotter(
	partitionCount uint8, snapshotBatchSize int, stateStorageManager statestorage.Manager,
	sideChannel sidechannel.SideChannel, taskManager task.TaskManager,
	publicationManager publication.PublicationManager, typeManager pgtypes.TypeManager,
	statsService *stats.Service,
) (*Snapshotter, error) {

	snapshotQueues := make([]chan SnapshotTask, partitionCount)
	for i := range snapshotQueues {
		snapshotQueues[i] = make(chan SnapshotTask, 128)
	}

	logger, err := logging.NewLogger("Snapshotter")
	if err != nil {
		return nil, err
	}

	s := &Snapshotter{
		partitionCount:    uint64(partitionCount),
		snapshotBatchSize: snapshotBatchSize,

		stateStorageManager: stateStorageManager,
		sideChannel:         sideChannel,
		taskManager:         taskManager,
		typeManager:         typeManager,
		publicationManager:  publicationManager,
		snapshotQueues:      snapshotQueues,
		logger:              logger,
		statsReporter:       statsService.NewReporter("streamer_snapshotter"),
		shutdownAwaiter:     waiting.NewMultiShutdownAwaiter(uint(partitionCount)),
		partitionStats:      make([]snapshotterPartitionStats, partitionCount),
	}

	s.stats.scheduler.partitionCount = uint(partitionCount)

	return s, nil
}

func (s *Snapshotter) EnqueueSnapshot(
	t SnapshotTask,
) error {

	enqueueSnapshotTask := func() {
		defer s.statsReporter.Report(s.stats)

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
		s.stats.scheduler.scheduled++
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
				case t := <-s.snapshotQueues[partition]:
					if err := s.snapshot(t, partition); err != nil {
						s.logger.Fatalf("snapshotting of task '%+v' failed: %+v", t, err)
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
	task SnapshotTask, partition int,
) error {

	if task.Chunk != nil {
		return s.snapshotChunk(task, partition)
	}
	return s.snapshotHypertable(task, partition)
}

func (s *Snapshotter) snapshotChunk(
	t SnapshotTask, partition int,
) error {

	defer s.statsReporter.Report(s.partitionStats[partition])
	s.partitionStats[partition].snapshots.chunks++

	alreadyPublished, err := s.publicationManager.ExistsTableInPublication(t.Chunk)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	if !alreadyPublished {
		if err := s.publicationManager.AttachTablesToPublication(t.Chunk); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	lsn, err := s.sideChannel.SnapshotChunkTable(
		s.typeManager.GetOrPlanRowDecoder, t.Chunk, s.snapshotBatchSize,
		func(lsn pgtypes.LSN, values map[string]any) error {
			s.partitionStats[partition].records.total++
			return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
				callback := func(handler eventhandlers.RecordReplicationEventHandler) error {
					return handler.OnReadEvent(lsn, t.Hypertable, t.Chunk, values)
				}
				if t.Xld != nil {
					callback = func(handler eventhandlers.RecordReplicationEventHandler) error {
						return handler.OnInsertEvent(*t.Xld, t.Hypertable, t.Chunk, values)
					}
				}
				notificator.NotifyRecordReplicationEventHandler(callback)
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
	t SnapshotTask, partition int,
) error {

	defer s.statsReporter.Report(s.partitionStats[partition])
	s.partitionStats[partition].snapshots.hypertables++

	// tableSnapshotState
	if err := s.stateStorageManager.SnapshotContextTransaction(
		*t.SnapshotName, true,
		func(snapshotContext *watermark.SnapshotContext) error {
			hypertableWatermark, created := snapshotContext.GetOrCreateWatermark(t.Hypertable)

			// Initialize the watermark or update the high watermark after a restart
			if created || t.nextSnapshotFetch {
				highWatermark, err := s.sideChannel.ReadSnapshotHighWatermark(
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
	if err := s.runSnapshotFetchBatch(t, partition); err != nil {
		return errors.Wrap(err, 0)
	}

	return s.stateStorageManager.SnapshotContextTransaction(
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
	t SnapshotTask, partition int,
) error {

	iteration := 0
	return s.sideChannel.FetchHypertableSnapshotBatch(
		s.typeManager.GetOrPlanRowDecoder, t.Hypertable, *t.SnapshotName, s.snapshotBatchSize,
		func(lsn pgtypes.LSN, values map[string]any) error {
			s.partitionStats[partition].records.total++
			iteration++
			if iteration > 100 {
				s.statsReporter.Report(s.stats)
				iteration = 0
			}
			return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
				notificator.NotifyRecordReplicationEventHandler(
					func(handler eventhandlers.RecordReplicationEventHandler) error {
						return handler.OnReadEvent(lsn, t.Hypertable, t.Chunk, values)
					},
				)
			})
		},
	)
}
