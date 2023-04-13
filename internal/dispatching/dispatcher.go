package dispatching

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"os"
	"time"
)

type Task = func(notificator Notificator)

type Notificator interface {
	NotifyBaseReplicationEventHandler(fn func(handler eventhandlers.BaseReplicationEventHandler) error)
	NotifySystemCatalogReplicationEventHandler(fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error)
	NotifyCompressionReplicationEventHandler(fn func(handler eventhandlers.CompressionReplicationEventHandler) error)
	NotifyHypertableReplicationEventHandler(fn func(handler eventhandlers.HypertableReplicationEventHandler) error)
	NotifyLogicalReplicationEventHandler(fn func(handler eventhandlers.LogicalReplicationEventHandler) error)
	NotifyChunkSnapshotEventHandler(fn func(handler eventhandlers.ChunkSnapshotEventHandler) error)
}

type Dispatcher struct {
	taskQueue           *supporting.Queue[Task]
	baseHandlers        []eventhandlers.BaseReplicationEventHandler
	catalogHandlers     []eventhandlers.SystemCatalogReplicationEventHandler
	compressionHandlers []eventhandlers.CompressionReplicationEventHandler
	hypertableHandlers  []eventhandlers.HypertableReplicationEventHandler
	logicalHandlers     []eventhandlers.LogicalReplicationEventHandler
	snapshotHandlers    []eventhandlers.ChunkSnapshotEventHandler
	shutdownAwaiter     *supporting.ShutdownAwaiter
	shutdownActive      bool
}

func NewDispatcher() *Dispatcher {
	d := &Dispatcher{
		taskQueue:           supporting.NewQueue[Task](),
		baseHandlers:        make([]eventhandlers.BaseReplicationEventHandler, 0),
		catalogHandlers:     make([]eventhandlers.SystemCatalogReplicationEventHandler, 0),
		compressionHandlers: make([]eventhandlers.CompressionReplicationEventHandler, 0),
		hypertableHandlers:  make([]eventhandlers.HypertableReplicationEventHandler, 0),
		logicalHandlers:     make([]eventhandlers.LogicalReplicationEventHandler, 0),
		snapshotHandlers:    make([]eventhandlers.ChunkSnapshotEventHandler, 0),
		shutdownAwaiter:     supporting.NewShutdownAwaiter(),
	}
	return d
}

func (d *Dispatcher) RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
	for _, candidate := range d.baseHandlers {
		if candidate == handler {
			return
		}
	}
	d.baseHandlers = append(d.baseHandlers, handler)

	if h, ok := handler.(eventhandlers.SystemCatalogReplicationEventHandler); ok {
		for _, candidate := range d.catalogHandlers {
			if candidate == h {
				return
			}
		}
		d.catalogHandlers = append(d.catalogHandlers, h)
	}

	if h, ok := handler.(eventhandlers.CompressionReplicationEventHandler); ok {
		for _, candidate := range d.compressionHandlers {
			if candidate == h {
				return
			}
		}
		d.compressionHandlers = append(d.compressionHandlers, h)
	}

	if h, ok := handler.(eventhandlers.HypertableReplicationEventHandler); ok {
		for _, candidate := range d.hypertableHandlers {
			if candidate == h {
				return
			}
		}
		d.hypertableHandlers = append(d.hypertableHandlers, h)
	}

	if h, ok := handler.(eventhandlers.LogicalReplicationEventHandler); ok {
		for _, candidate := range d.logicalHandlers {
			if candidate == h {
				return
			}
		}
		d.logicalHandlers = append(d.logicalHandlers, h)
	}

	if h, ok := handler.(eventhandlers.ChunkSnapshotEventHandler); ok {
		for _, candidate := range d.snapshotHandlers {
			if candidate == h {
				return
			}
		}
		d.snapshotHandlers = append(d.snapshotHandlers, h)
	}
}

func (d *Dispatcher) UnregisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
	for index, candidate := range d.baseHandlers {
		if candidate == handler {
			// Erase element (zero value) to prevent memory leak
			d.baseHandlers[index] = nil
			d.baseHandlers = append(d.baseHandlers[:index], d.baseHandlers[index+1:]...)
		}
	}

	if h, ok := handler.(eventhandlers.SystemCatalogReplicationEventHandler); ok {
		for index, candidate := range d.catalogHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.catalogHandlers[index] = nil
				d.catalogHandlers = append(d.catalogHandlers[:index], d.catalogHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(eventhandlers.CompressionReplicationEventHandler); ok {
		for index, candidate := range d.compressionHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.compressionHandlers[index] = nil
				d.compressionHandlers = append(d.compressionHandlers[:index], d.compressionHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(eventhandlers.HypertableReplicationEventHandler); ok {
		for index, candidate := range d.hypertableHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.hypertableHandlers[index] = nil
				d.hypertableHandlers = append(d.hypertableHandlers[:index], d.hypertableHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(eventhandlers.LogicalReplicationEventHandler); ok {
		for index, candidate := range d.logicalHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.logicalHandlers[index] = nil
				d.logicalHandlers = append(d.logicalHandlers[:index], d.logicalHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(eventhandlers.ChunkSnapshotEventHandler); ok {
		for index, candidate := range d.snapshotHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.snapshotHandlers[index] = nil
				d.snapshotHandlers = append(d.snapshotHandlers[:index], d.snapshotHandlers[index+1:]...)
			}
		}
	}
}

func (d *Dispatcher) StartDispatcher() {
	notificator := &notificatorImpl{dispatcher: d}
	go func() {
		for {
			select {
			case <-d.shutdownAwaiter.AwaitShutdownChan():
				goto finish
			default:
			}

			task := d.taskQueue.Pop()
			if task != nil {
				task(notificator)
			} else {
				time.Sleep(time.Millisecond * 10)
			}
		}

	finish:
		d.shutdownAwaiter.SignalDone()
	}()
}

func (d *Dispatcher) StopDispatcher() error {
	d.shutdownActive = true
	d.shutdownAwaiter.SignalShutdown()
	return d.shutdownAwaiter.AwaitDone()
}

func (d *Dispatcher) EnqueueTask(task Task) error {
	if d.shutdownActive {
		return fmt.Errorf("shutdown active, draining only")
	}
	d.taskQueue.Push(task)
	return nil
}

func (d *Dispatcher) EnqueueTaskAndWait(task Task) error {
	if d.shutdownActive {
		return fmt.Errorf("shutdown active, draining only")
	}
	done := supporting.NewWaiter()
	d.taskQueue.Push(func(notificator Notificator) {
		task(notificator)
		done.Signal()
	})
	return done.Await()
}

type notificatorImpl struct {
	dispatcher *Dispatcher
}

func (n *notificatorImpl) NotifyBaseReplicationEventHandler(
	fn func(handler eventhandlers.BaseReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.baseHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifySystemCatalogReplicationEventHandler(
	fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.catalogHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifyCompressionReplicationEventHandler(fn func(
	handler eventhandlers.CompressionReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.compressionHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifyHypertableReplicationEventHandler(
	fn func(handler eventhandlers.HypertableReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.hypertableHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifyLogicalReplicationEventHandler(
	fn func(handler eventhandlers.LogicalReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.logicalHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifyChunkSnapshotEventHandler(
	fn func(handler eventhandlers.ChunkSnapshotEventHandler) error) {

	for _, handler := range n.dispatcher.snapshotHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) handleError(err error) {
	fmt.Fprintf(os.Stderr, "Error while dispatching event: %v\n", err)
}
