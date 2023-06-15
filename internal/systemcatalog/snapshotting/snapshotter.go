package snapshotting

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
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
	replicationContext *context.ReplicationContext
	snapshotQueues     []chan SnapshotTask
	shutdownAwaiter    *supporting.MultiShutdownAwaiter
	logger             *logging.Logger
}

func NewSnapshotter(partitionCount uint8, replicationContext *context.ReplicationContext) (*Snapshotter, error) {
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
		snapshotQueues:     snapshotQueues,
		logger:             logger,
		shutdownAwaiter:    supporting.NewMultiShutdownAwaiter(uint(partitionCount)),
	}, nil
}

func (s *Snapshotter) EnqueueSnapshot(task SnapshotTask) error {
	enqueueSnapshotTask := func() {
		// Partition calculation
		hasher := fnv.New64a()
		hasher.Write([]byte(task.Hypertable.CanonicalName()))
		partition := int(hasher.Sum64() % s.partitionCount)

		// Enqueue the actual task
		s.snapshotQueues[partition] <- task
	}

	// Notify of snapshotting to save incoming events
	if task.Chunk != nil {
		err := s.replicationContext.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnChunkSnapshotStartedEvent(task.Hypertable, task.Chunk)
			})
			enqueueSnapshotTask()
		})
		if err != nil {
			return err
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

func (s *Snapshotter) snapshot(task SnapshotTask) error {
	if task.Chunk != nil {
		return s.snapshotChunk(task)
	}
	return s.snapshotHypertable(task)
}

func (s *Snapshotter) snapshotChunk(task SnapshotTask) error {
	if err := s.replicationContext.AttachTablesToPublication(task.Chunk); err != nil {
		return errors.Wrap(err, 0)
	}

	lsn, err := s.replicationContext.SnapshotChunkTable(
		task.Chunk, func(lsn pgtypes.LSN, values map[string]any) error {
			return s.replicationContext.EnqueueTask(func(notificator context.Notificator) {
				callback := func(handler eventhandlers.HypertableReplicationEventHandler) error {
					return handler.OnReadEvent(lsn, task.Hypertable, task.Chunk, values)
				}
				if task.Xld != nil {
					callback = func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnInsertEvent(*task.Xld, task.Hypertable, task.Chunk, values)
					}
				}
				notificator.NotifyHypertableReplicationEventHandler(callback)
			})
		},
	)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return s.replicationContext.EnqueueTaskAndWait(func(notificator context.Notificator) {
		notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
			return handler.OnChunkSnapshotFinishedEvent(task.Hypertable, task.Chunk, lsn)
		})
	})
}

func (s *Snapshotter) snapshotHypertable(task SnapshotTask) error {
	// tableSnapshotState
	if err := s.replicationContext.SnapshotContextTransaction(
		*task.SnapshotName, true,
		func(snapshotContext *watermark.SnapshotContext) error {
			watermark, created := snapshotContext.GetOrCreateWatermark(task.Hypertable)

			// Initialize the watermark or update the high watermark after a restart
			if created || task.nextSnapshotFetch {
				highWatermark, err := s.replicationContext.ReadSnapshotHighWatermark(task.Hypertable, *task.SnapshotName)
				if err != nil {
					return errors.Wrap(err, 0)
				}

				watermark.SetHighWatermark(highWatermark)
			}

			return nil
		},
	); err != nil {
		return errors.Wrap(err, 0)
	}

	// Kick off snapshot fetching
	if err := s.runSnapshotFetchBatch(task); err != nil {
		return err
	}

	return s.replicationContext.SnapshotContextTransaction(
		*task.SnapshotName, false,
		func(snapshotContext *watermark.SnapshotContext) error {
			watermark, present := snapshotContext.GetWatermark(task.Hypertable)
			if !present {
				return errors.Errorf(
					"illegal watermark state for hypertable '%s'", task.Hypertable.CanonicalName(),
				)
			}

			if watermark.Complete() {
				return s.replicationContext.EnqueueTaskAndWait(func(notificator context.Notificator) {
					notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
						return handler.OnHypertableSnapshotFinishedEvent(*task.SnapshotName, task.Hypertable)
					})
				})
			}

			return s.EnqueueSnapshot(SnapshotTask{
				Hypertable:        task.Hypertable,
				SnapshotName:      task.SnapshotName,
				nextSnapshotFetch: true,
			})
		},
	)
}

func (s *Snapshotter) runSnapshotFetchBatch(task SnapshotTask) error {
	return s.replicationContext.FetchHypertableSnapshotBatch(
		task.Hypertable, *task.SnapshotName,
		func(lsn pgtypes.LSN, values map[string]any) error {
			return s.replicationContext.EnqueueTask(func(notificator context.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnReadEvent(lsn, task.Hypertable, task.Chunk, values)
					},
				)
			})
		},
	)
}
