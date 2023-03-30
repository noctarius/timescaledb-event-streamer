package eventhandler

import (
	"fmt"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"os"
)

type Task = func(notificator Notificator)

type Notificator interface {
	NotifyBaseReplicationEventHandler(fn func(handler BaseReplicationEventHandler) error)
	NotifySystemCatalogReplicationEventHandler(fn func(handler SystemCatalogReplicationEventHandler) error)
	NotifyCompressionReplicationEventHandler(fn func(handler CompressionReplicationEventHandler) error)
	NotifyHypertableReplicationEventHandler(fn func(handler HypertableReplicationEventHandler) error)
	NotifyLogicalReplicationEventHandler(fn func(handler LogicalReplicationEventHandler) error)
	NotifyChunkSnapshotEventHandler(fn func(handler ChunkSnapshotEventHandler) error)
}

type Dispatcher struct {
	taskQueue           chan Task
	baseHandlers        []BaseReplicationEventHandler
	catalogHandlers     []SystemCatalogReplicationEventHandler
	compressionHandlers []CompressionReplicationEventHandler
	hypertableHandlers  []HypertableReplicationEventHandler
	logicalHandlers     []LogicalReplicationEventHandler
	snapshotHandlers    []ChunkSnapshotEventHandler
	shutdownAwaiter     *supporting.ShutdownAwaiter
	shutdownActive      bool
}

func NewDispatcher(queueLength int) *Dispatcher {
	d := &Dispatcher{
		taskQueue:           make(chan Task, queueLength),
		baseHandlers:        make([]BaseReplicationEventHandler, 0),
		catalogHandlers:     make([]SystemCatalogReplicationEventHandler, 0),
		compressionHandlers: make([]CompressionReplicationEventHandler, 0),
		hypertableHandlers:  make([]HypertableReplicationEventHandler, 0),
		logicalHandlers:     make([]LogicalReplicationEventHandler, 0),
		snapshotHandlers:    make([]ChunkSnapshotEventHandler, 0),
		shutdownAwaiter:     supporting.NewShutdownAwaiter(),
	}
	return d
}

func (d *Dispatcher) RegisterReplicationEventHandler(handler BaseReplicationEventHandler) {
	for _, candidate := range d.baseHandlers {
		if candidate == handler {
			return
		}
	}
	d.baseHandlers = append(d.baseHandlers, handler)

	if h, ok := handler.(SystemCatalogReplicationEventHandler); ok {
		for _, candidate := range d.catalogHandlers {
			if candidate == h {
				return
			}
		}
		d.catalogHandlers = append(d.catalogHandlers, h)
	}

	if h, ok := handler.(CompressionReplicationEventHandler); ok {
		for _, candidate := range d.compressionHandlers {
			if candidate == h {
				return
			}
		}
		d.compressionHandlers = append(d.compressionHandlers, h)
	}

	if h, ok := handler.(HypertableReplicationEventHandler); ok {
		for _, candidate := range d.hypertableHandlers {
			if candidate == h {
				return
			}
		}
		d.hypertableHandlers = append(d.hypertableHandlers, h)
	}

	if h, ok := handler.(LogicalReplicationEventHandler); ok {
		for _, candidate := range d.logicalHandlers {
			if candidate == h {
				return
			}
		}
		d.logicalHandlers = append(d.logicalHandlers, h)
	}

	if h, ok := handler.(ChunkSnapshotEventHandler); ok {
		for _, candidate := range d.snapshotHandlers {
			if candidate == h {
				return
			}
		}
		d.snapshotHandlers = append(d.snapshotHandlers, h)
	}
}

func (d *Dispatcher) UnregisterReplicationEventHandler(handler BaseReplicationEventHandler) {
	for index, candidate := range d.baseHandlers {
		if candidate == handler {
			// Erase element (zero value) to prevent memory leak
			d.baseHandlers[index] = nil
			d.baseHandlers = append(d.baseHandlers[:index], d.baseHandlers[index+1:]...)
		}
	}

	if h, ok := handler.(SystemCatalogReplicationEventHandler); ok {
		for index, candidate := range d.catalogHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.catalogHandlers[index] = nil
				d.catalogHandlers = append(d.catalogHandlers[:index], d.catalogHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(CompressionReplicationEventHandler); ok {
		for index, candidate := range d.compressionHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.compressionHandlers[index] = nil
				d.compressionHandlers = append(d.compressionHandlers[:index], d.compressionHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(HypertableReplicationEventHandler); ok {
		for index, candidate := range d.hypertableHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.hypertableHandlers[index] = nil
				d.hypertableHandlers = append(d.hypertableHandlers[:index], d.hypertableHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(LogicalReplicationEventHandler); ok {
		for index, candidate := range d.logicalHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.logicalHandlers[index] = nil
				d.logicalHandlers = append(d.logicalHandlers[:index], d.logicalHandlers[index+1:]...)
			}
		}
	}

	if h, ok := handler.(ChunkSnapshotEventHandler); ok {
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
			case task := <-d.taskQueue:
				task(notificator)

			case <-d.shutdownAwaiter.AwaitShutdownChan():
				goto finish
			}
		}

	finish:
		d.shutdownAwaiter.SignalDone()
	}()
}

func (d *Dispatcher) StopDispatcher() {
	d.shutdownActive = true
	d.shutdownAwaiter.SignalShutdown()
	d.shutdownAwaiter.AwaitDone()
}

func (d *Dispatcher) EnqueueTask(task Task) error {
	if d.shutdownActive {
		return fmt.Errorf("shutdown active, draining only")
	}
	d.taskQueue <- task
	return nil
}

func (d *Dispatcher) EnqueueTaskAndWait(task Task) error {
	if d.shutdownActive {
		return fmt.Errorf("shutdown active, draining only")
	}
	done := supporting.NewWaiter()
	d.taskQueue <- func(notificator Notificator) {
		task(notificator)
		done.Signal()
	}
	done.Await()
	return nil
}

type notificatorImpl struct {
	dispatcher *Dispatcher
}

func (n *notificatorImpl) NotifyBaseReplicationEventHandler(
	fn func(handler BaseReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.baseHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifySystemCatalogReplicationEventHandler(
	fn func(handler SystemCatalogReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.catalogHandlers {
		if h, ok := handler.(SystemCatalogReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyCompressionReplicationEventHandler(fn func(
	handler CompressionReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.compressionHandlers {
		if h, ok := handler.(CompressionReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyHypertableReplicationEventHandler(
	fn func(handler HypertableReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.hypertableHandlers {
		if h, ok := handler.(HypertableReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyLogicalReplicationEventHandler(
	fn func(handler LogicalReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.logicalHandlers {
		if h, ok := handler.(LogicalReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyChunkSnapshotEventHandler(
	fn func(handler ChunkSnapshotEventHandler) error) {

	for _, handler := range n.dispatcher.snapshotHandlers {
		if h, ok := handler.(ChunkSnapshotEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) handleError(err error) {
	fmt.Fprintf(os.Stderr, "Error while dispatching event: %v\n", err)
}
