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

package taskmanager

import (
	stderrors "errors"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/containers"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"sync/atomic"
)

type taskManager struct {
	logger              *logging.Logger
	taskQueue           *containers.UnboundedChannel[task.Task]
	baseHandlers        []eventhandlers.BaseReplicationEventHandler
	catalogHandlers     []eventhandlers.SystemCatalogReplicationEventHandler
	compressionHandlers []eventhandlers.CompressionReplicationEventHandler
	recordHandlers      []eventhandlers.RecordReplicationEventHandler
	logicalHandlers     []eventhandlers.LogicalReplicationEventHandler
	snapshotHandlers    []eventhandlers.SnapshottingEventHandler
	shutdownAwaiter     *waiting.ShutdownAwaiter
	shutdownActive      atomic.Bool
}

func NewTaskManager(
	config *spiconfig.Config,
) (task.TaskManager, error) {

	logger, err := logging.NewLogger("TaskDispatcher")
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	initialQueueCapacity := spiconfig.GetOrDefault[int](
		config, spiconfig.PropertyDispatcherInitialQueueCapacity, 16384,
	)

	d := &taskManager{
		logger:              logger,
		taskQueue:           containers.MakeUnboundedChannel[task.Task](initialQueueCapacity),
		baseHandlers:        make([]eventhandlers.BaseReplicationEventHandler, 0),
		catalogHandlers:     make([]eventhandlers.SystemCatalogReplicationEventHandler, 0),
		compressionHandlers: make([]eventhandlers.CompressionReplicationEventHandler, 0),
		recordHandlers:      make([]eventhandlers.RecordReplicationEventHandler, 0),
		logicalHandlers:     make([]eventhandlers.LogicalReplicationEventHandler, 0),
		snapshotHandlers:    make([]eventhandlers.SnapshottingEventHandler, 0),
		shutdownAwaiter:     waiting.NewShutdownAwaiter(),
		shutdownActive:      atomic.Bool{},
	}
	return d, nil
}

func (d *taskManager) RegisterReplicationEventHandler(
	handler eventhandlers.BaseReplicationEventHandler,
) {

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

	if h, ok := handler.(eventhandlers.RecordReplicationEventHandler); ok {
		for _, candidate := range d.recordHandlers {
			if candidate == h {
				return
			}
		}
		d.recordHandlers = append(d.recordHandlers, h)
	}

	if h, ok := handler.(eventhandlers.LogicalReplicationEventHandler); ok {
		for _, candidate := range d.logicalHandlers {
			if candidate == h {
				return
			}
		}
		d.logicalHandlers = append(d.logicalHandlers, h)
	}

	if h, ok := handler.(eventhandlers.SnapshottingEventHandler); ok {
		for _, candidate := range d.snapshotHandlers {
			if candidate == h {
				return
			}
		}
		d.snapshotHandlers = append(d.snapshotHandlers, h)
	}
}

func (d *taskManager) UnregisterReplicationEventHandler(
	handler eventhandlers.BaseReplicationEventHandler,
) {

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

	if h, ok := handler.(eventhandlers.RecordReplicationEventHandler); ok {
		for index, candidate := range d.recordHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.recordHandlers[index] = nil
				d.recordHandlers = append(d.recordHandlers[:index], d.recordHandlers[index+1:]...)
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

	if h, ok := handler.(eventhandlers.SnapshottingEventHandler); ok {
		for index, candidate := range d.snapshotHandlers {
			if candidate == h {
				// Erase element (zero value) to prevent memory leak
				d.snapshotHandlers[index] = nil
				d.snapshotHandlers = append(d.snapshotHandlers[:index], d.snapshotHandlers[index+1:]...)
			}
		}
	}
}

func (d *taskManager) StartDispatcher() {
	go func() {
		notificator := &notificator{dispatcher: d}
		for {
			select {
			case <-d.shutdownAwaiter.AwaitShutdownChan():
				d.logger.Infof("TaskManager shutting down")
				goto finish
			case task := <-d.taskQueue.ReceiveChannel():
				if task != nil {
					task(notificator)
				}
			}
		}

	finish:
		d.shutdownAwaiter.SignalDone()
	}()
}

func (d *taskManager) StopDispatcher() error {
	d.shutdownActive.Store(true)
	d.shutdownAwaiter.SignalShutdown()
	d.taskQueue.Close()
	return d.shutdownAwaiter.AwaitDone()
}

func (d *taskManager) EnqueueTask(
	task task.Task,
) error {

	if d.shutdownActive.Load() {
		return fmt.Errorf("shutdown active, draining only")
	}
	d.taskQueue.Send(task)
	return nil
}

func (d *taskManager) EnqueueTaskAndWait(
	t task.Task,
) error {

	if d.shutdownActive.Load() {
		return fmt.Errorf("shutdown active, draining only")
	}
	done := waiting.NewWaiter()
	d.taskQueue.Send(func(notificator task.Notificator) {
		t(notificator)
		done.Signal()
	})
	return done.Await()
}

func (d *taskManager) RunTask(
	task task.Task,
) error {

	notificator := &immediateNotificator{dispatcher: d}
	task(notificator)
	if notificator.errors == nil || len(notificator.errors) == 0 {
		return nil
	}
	return stderrors.Join(notificator.errors...)
}

type notificator struct {
	dispatcher *taskManager
}

func (n *notificator) NotifyBaseReplicationEventHandler(
	fn func(handler eventhandlers.BaseReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.baseHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificator) NotifySystemCatalogReplicationEventHandler(
	fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.catalogHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificator) NotifyCompressionReplicationEventHandler(
	fn func(handler eventhandlers.CompressionReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.compressionHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificator) NotifyRecordReplicationEventHandler(
	fn func(handler eventhandlers.RecordReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.recordHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificator) NotifyLogicalReplicationEventHandler(
	fn func(handler eventhandlers.LogicalReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.logicalHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificator) NotifySnapshottingEventHandler(
	fn func(handler eventhandlers.SnapshottingEventHandler) error,
) {

	for _, handler := range n.dispatcher.snapshotHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificator) handleError(
	err error,
) {

	errMsg := err.Error()
	if e, ok := err.(*errors.Error); ok {
		errMsg = e.ErrorStack()
	}

	n.dispatcher.logger.Warnf("Error while dispatching event: %s\n", errMsg)
}

type immediateNotificator struct {
	dispatcher *taskManager
	errors     []error
}

func (n *immediateNotificator) NotifyBaseReplicationEventHandler(
	fn func(handler eventhandlers.BaseReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.baseHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificator) NotifySystemCatalogReplicationEventHandler(
	fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.catalogHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificator) NotifyCompressionReplicationEventHandler(
	fn func(handler eventhandlers.CompressionReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.compressionHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificator) NotifyRecordReplicationEventHandler(
	fn func(handler eventhandlers.RecordReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.recordHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificator) NotifyLogicalReplicationEventHandler(
	fn func(handler eventhandlers.LogicalReplicationEventHandler) error,
) {

	for _, handler := range n.dispatcher.logicalHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificator) NotifySnapshottingEventHandler(
	fn func(handler eventhandlers.SnapshottingEventHandler) error,
) {

	for _, handler := range n.dispatcher.snapshotHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificator) handleError(
	err error,
) {

	if e, ok := err.(*errors.Error); !ok {
		err = errors.Wrap(e, 0)
	}
	n.errors = append(n.errors, err)
}
