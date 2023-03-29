package snapshotting

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/replication/channel"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"hash/fnv"
	"sync"
	"time"
)

var logger = logging.NewLogger("Snapshotter")

type SnapshotTask struct {
	Hypertable *model.Hypertable
	Chunk      *model.Chunk
}

type Snapshotter struct {
	partitionCount    uint64
	dispatcher        *eventhandler.Dispatcher
	queryAdapter      channel.QueryAdapter
	snapshotQueues    []chan SnapshotTask
	shutdownStarts    []chan bool
	shutdownWaitGroup sync.WaitGroup
}

func NewSnapshotter(partitionCount uint8, queryAdapter channel.QueryAdapter,
	dispatcher *eventhandler.Dispatcher) *Snapshotter {

	snapshotQueues := make([]chan SnapshotTask, partitionCount)
	shutdownStarts := make([]chan bool, partitionCount)
	for i := range snapshotQueues {
		snapshotQueues[i] = make(chan SnapshotTask, 128)
		shutdownStarts[i] = make(chan bool, 1)
	}

	return &Snapshotter{
		partitionCount:    uint64(partitionCount),
		dispatcher:        dispatcher,
		queryAdapter:      queryAdapter,
		snapshotQueues:    snapshotQueues,
		shutdownStarts:    shutdownStarts,
		shutdownWaitGroup: sync.WaitGroup{},
	}
}

func (s *Snapshotter) EnqueueSnapshot(task SnapshotTask) error {
	// Notify of snapshotting to save incoming events
	if task.Chunk != nil {
		err := s.dispatcher.EnqueueTaskAndWait(func(notificator eventhandler.Notificator) {
			notificator.NotifyChunkSnapshotEventHandler(func(handler eventhandler.ChunkSnapshotEventHandler) error {
				return handler.OnChunkSnapshotStartedEvent(task.Hypertable, task.Chunk)
			})
		})
		if err != nil {
			return err
		}
	}

	// Partition calculation
	hasher := fnv.New64a()
	hasher.Write([]byte(task.Hypertable.CanonicalName()))
	partition := int(hasher.Sum64() % s.partitionCount)

	// Enqueue the actual task
	s.snapshotQueues[partition] <- task
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
				case <-s.shutdownStarts[partition]:
					goto shutdown
				case <-time.After(time.Second * 5):
					// timeout, keep running
					logger.Printf("Partition handler: %d", partition)
				}
			}

		shutdown:
			s.shutdownWaitGroup.Done()
		}(i)
	}
	s.shutdownWaitGroup.Add(int(s.partitionCount))
}

func (s *Snapshotter) StopSnapshotter() {
	for i := 0; i < int(s.partitionCount); i++ {
		s.shutdownStarts[i] <- true
	}
	s.shutdownWaitGroup.Wait()
}

func (s *Snapshotter) snapshot(task SnapshotTask) error {
	if task.Chunk != nil {
		return s.snapshotChunk(task)
	}
	return s.snapshotHypertable(task)
}

func (s *Snapshotter) snapshotChunk(task SnapshotTask) error {
	if err := s.queryAdapter.AttachChunkToPublication(task.Chunk); err != nil {
		return errors.Wrap(err, 0)
	}

	lsn, err := s.queryAdapter.SnapshotTable(
		task.Chunk.CanonicalName(), nil,
		func(lsn pglogrepl.LSN, values map[string]any) error {
			return s.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnReadEvent(lsn, task.Hypertable, task.Chunk, values)
					},
				)
			})
		},
	)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return s.dispatcher.EnqueueTaskAndWait(func(notificator eventhandler.Notificator) {
		notificator.NotifyChunkSnapshotEventHandler(func(handler eventhandler.ChunkSnapshotEventHandler) error {
			return handler.OnChunkSnapshotFinishedEvent(task.Hypertable, task.Chunk, lsn)
		})
	})
}

func (s *Snapshotter) snapshotHypertable(task SnapshotTask) error {
	return nil // TODO
}
