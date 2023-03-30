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
	taskQueue       chan Task
	handlers        []BaseReplicationEventHandler
	shutdownAwaiter *supporting.ShutdownAwaiter
	shutdownActive  bool
}

func NewDispatcher(queueLength int) *Dispatcher {
	d := &Dispatcher{
		taskQueue:       make(chan Task, queueLength),
		handlers:        make([]BaseReplicationEventHandler, 0),
		shutdownAwaiter: supporting.NewShutdownAwaiter(),
	}
	return d
}

func (d *Dispatcher) RegisterReplicationEventHandler(handler BaseReplicationEventHandler) {
	for _, candidate := range d.handlers {
		if candidate == handler {
			return
		}
	}
	d.handlers = append(d.handlers, handler)
}

func (d *Dispatcher) UnregisterReplicationEventHandler(handler BaseReplicationEventHandler) {
	for index, candidate := range d.handlers {
		if candidate == handler {
			// Erase element (zero value) to prevent memory leak
			d.handlers[index] = nil
			d.handlers = append(d.handlers[:index], d.handlers[index+1:]...)
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

	for _, handler := range n.dispatcher.handlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) NotifySystemCatalogReplicationEventHandler(
	fn func(handler SystemCatalogReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.handlers {
		if h, ok := handler.(SystemCatalogReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyCompressionReplicationEventHandler(fn func(
	handler CompressionReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.handlers {
		if h, ok := handler.(CompressionReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyHypertableReplicationEventHandler(
	fn func(handler HypertableReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.handlers {
		if h, ok := handler.(HypertableReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyLogicalReplicationEventHandler(
	fn func(handler LogicalReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.handlers {
		if h, ok := handler.(LogicalReplicationEventHandler); ok {
			if err := fn(h); err != nil {
				n.handleError(err)
			}
		}
	}
}

func (n *notificatorImpl) NotifyChunkSnapshotEventHandler(
	fn func(handler ChunkSnapshotEventHandler) error) {

	for _, handler := range n.dispatcher.handlers {
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
