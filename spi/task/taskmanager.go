package task

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
)

type TaskManager interface {
	StartDispatcher()
	StopDispatcher() error
	RegisterReplicationEventHandler(
		handler eventhandlers.BaseReplicationEventHandler,
	)
	EnqueueTask(
		task Task,
	) error
	RunTask(
		task Task,
	) error
	EnqueueTaskAndWait(
		task Task,
	) error
}
