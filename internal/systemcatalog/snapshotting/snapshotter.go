package snapshotting

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/dispatching"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/channels"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"hash/fnv"
	"time"
)

var logger = logging.NewLogger("Snapshotter")

type SnapshotTask struct {
	Hypertable *systemcatalog.Hypertable
	Chunk      *systemcatalog.Chunk
}

type Snapshotter struct {
	partitionCount  uint64
	dispatcher      *dispatching.Dispatcher
	sideChannel     channels.SideChannel
	snapshotQueues  []chan SnapshotTask
	shutdownAwaiter *supporting.MultiShutdownAwaiter
}

func NewSnapshotter(partitionCount uint8, sideChannel channels.SideChannel,
	dispatcher *dispatching.Dispatcher) *Snapshotter {

	snapshotQueues := make([]chan SnapshotTask, partitionCount)
	for i := range snapshotQueues {
		snapshotQueues[i] = make(chan SnapshotTask, 128)
	}

	return &Snapshotter{
		partitionCount:  uint64(partitionCount),
		dispatcher:      dispatcher,
		sideChannel:     sideChannel,
		snapshotQueues:  snapshotQueues,
		shutdownAwaiter: supporting.NewMultiShutdownAwaiter(uint(partitionCount)),
	}
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
		err := s.dispatcher.EnqueueTask(func(notificator dispatching.Notificator) {
			notificator.NotifyChunkSnapshotEventHandler(func(handler eventhandlers.ChunkSnapshotEventHandler) error {
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
						logger.Fatalf("snapshotting of task '%+v' failed: %+v", task, err)
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
	if err := s.sideChannel.AttachChunkToPublication(task.Chunk); err != nil {
		return errors.Wrap(err, 0)
	}

	lsn, err := s.sideChannel.SnapshotTable(
		task.Chunk.CanonicalName(), nil,
		func(lsn pglogrepl.LSN, values map[string]any) error {
			return s.dispatcher.EnqueueTask(func(notificator dispatching.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandlers.HypertableReplicationEventHandler) error {
						return handler.OnReadEvent(lsn, task.Hypertable, task.Chunk, values)
					},
				)
			})
		},
	)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return s.dispatcher.EnqueueTaskAndWait(func(notificator dispatching.Notificator) {
		notificator.NotifyChunkSnapshotEventHandler(func(handler eventhandlers.ChunkSnapshotEventHandler) error {
			return handler.OnChunkSnapshotFinishedEvent(task.Hypertable, task.Chunk, lsn)
		})
	})
}

func (s *Snapshotter) snapshotHypertable(task SnapshotTask) error {
	return nil // TODO
}
