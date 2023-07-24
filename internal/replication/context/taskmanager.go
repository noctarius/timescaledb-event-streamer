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

package context

import (
	stderrors "errors"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
)

type Task = func(notificator Notificator)

type Notificator interface {
	NotifyBaseReplicationEventHandler(fn func(handler eventhandlers.BaseReplicationEventHandler) error)
	NotifySystemCatalogReplicationEventHandler(fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error)
	NotifyCompressionReplicationEventHandler(fn func(handler eventhandlers.CompressionReplicationEventHandler) error)
	NotifyHypertableReplicationEventHandler(fn func(handler eventhandlers.HypertableReplicationEventHandler) error)
	NotifyLogicalReplicationEventHandler(fn func(handler eventhandlers.LogicalReplicationEventHandler) error)
	NotifySnapshottingEventHandler(fn func(handler eventhandlers.SnapshottingEventHandler) error)
}

type TaskManager interface {
	RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler)
	EnqueueTask(task Task) error
	RunTask(task Task) error
	EnqueueTaskAndWait(task Task) error
}

type taskManager struct {
	logger              *logging.Logger
	taskQueue           *supporting.Channel[Task]
	baseHandlers        []eventhandlers.BaseReplicationEventHandler
	catalogHandlers     []eventhandlers.SystemCatalogReplicationEventHandler
	compressionHandlers []eventhandlers.CompressionReplicationEventHandler
	hypertableHandlers  []eventhandlers.HypertableReplicationEventHandler
	logicalHandlers     []eventhandlers.LogicalReplicationEventHandler
	snapshotHandlers    []eventhandlers.SnapshottingEventHandler
	shutdownAwaiter     *waiting.ShutdownAwaiter
	shutdownActive      bool
}

func newTaskManager(config *spiconfig.Config) (*taskManager, error) {
	logger, err := logging.NewLogger("TaskDispatcher")
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	maxQueueSize := spiconfig.GetOrDefault[int](config, spiconfig.PropertyDispatcherMaxQueueSize, 4096)

	d := &taskManager{
		logger:              logger,
		taskQueue:           supporting.NewChannel[Task](maxQueueSize),
		baseHandlers:        make([]eventhandlers.BaseReplicationEventHandler, 0),
		catalogHandlers:     make([]eventhandlers.SystemCatalogReplicationEventHandler, 0),
		compressionHandlers: make([]eventhandlers.CompressionReplicationEventHandler, 0),
		hypertableHandlers:  make([]eventhandlers.HypertableReplicationEventHandler, 0),
		logicalHandlers:     make([]eventhandlers.LogicalReplicationEventHandler, 0),
		snapshotHandlers:    make([]eventhandlers.SnapshottingEventHandler, 0),
		shutdownAwaiter:     waiting.NewShutdownAwaiter(),
	}
	return d, nil
}

func (d *taskManager) RegisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
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

	if h, ok := handler.(eventhandlers.SnapshottingEventHandler); ok {
		for _, candidate := range d.snapshotHandlers {
			if candidate == h {
				return
			}
		}
		d.snapshotHandlers = append(d.snapshotHandlers, h)
	}
}

func (d *taskManager) UnregisterReplicationEventHandler(handler eventhandlers.BaseReplicationEventHandler) {
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
		notificator := &notificatorImpl{dispatcher: d}
		for {
			select {
			case <-d.shutdownAwaiter.AwaitShutdownChan():
				goto finish
			case task := <-d.taskQueue.ReadChannel():
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
	d.shutdownActive = true
	d.shutdownAwaiter.SignalShutdown()
	d.taskQueue.Close()
	return d.shutdownAwaiter.AwaitDone()
}

func (d *taskManager) EnqueueTask(task Task) error {
	if d.shutdownActive {
		return fmt.Errorf("shutdown active, draining only")
	}
	d.taskQueue.Write(task)
	return nil
}

func (d *taskManager) EnqueueTaskAndWait(task Task) error {
	if d.shutdownActive {
		return fmt.Errorf("shutdown active, draining only")
	}
	done := waiting.NewWaiter()
	d.taskQueue.Write(func(notificator Notificator) {
		task(notificator)
		done.Signal()
	})
	return done.Await()
}

func (d *taskManager) RunTask(task Task) error {
	notificator := &immediateNotificatorImpl{dispatcher: d}
	task(notificator)
	if notificator.errors == nil || len(notificator.errors) == 0 {
		return nil
	}
	return stderrors.Join(notificator.errors...)
}

type notificatorImpl struct {
	dispatcher *taskManager
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

func (n *notificatorImpl) NotifySnapshottingEventHandler(
	fn func(handler eventhandlers.SnapshottingEventHandler) error) {

	for _, handler := range n.dispatcher.snapshotHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *notificatorImpl) handleError(err error) {
	errMsg := err.Error()
	if e, ok := err.(*errors.Error); ok {
		errMsg = e.ErrorStack()
	}

	n.dispatcher.logger.Warnf("Error while dispatching event: %s\n", errMsg)
}

type immediateNotificatorImpl struct {
	dispatcher *taskManager
	errors     []error
}

func (n *immediateNotificatorImpl) NotifyBaseReplicationEventHandler(
	fn func(handler eventhandlers.BaseReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.baseHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificatorImpl) NotifySystemCatalogReplicationEventHandler(
	fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.catalogHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificatorImpl) NotifyCompressionReplicationEventHandler(fn func(
	handler eventhandlers.CompressionReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.compressionHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificatorImpl) NotifyHypertableReplicationEventHandler(
	fn func(handler eventhandlers.HypertableReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.hypertableHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificatorImpl) NotifyLogicalReplicationEventHandler(
	fn func(handler eventhandlers.LogicalReplicationEventHandler) error) {

	for _, handler := range n.dispatcher.logicalHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificatorImpl) NotifySnapshottingEventHandler(
	fn func(handler eventhandlers.SnapshottingEventHandler) error) {

	for _, handler := range n.dispatcher.snapshotHandlers {
		if err := fn(handler); err != nil {
			n.handleError(err)
		}
	}
}

func (n *immediateNotificatorImpl) handleError(err error) {
	if e, ok := err.(*errors.Error); !ok {
		err = errors.Wrap(e, 0)
	}
	n.errors = append(n.errors, err)
}
