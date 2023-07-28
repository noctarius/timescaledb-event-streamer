package task

import "github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"

type Task = func(notificator Notificator)

type Notificator interface {
	NotifyBaseReplicationEventHandler(
		fn func(handler eventhandlers.BaseReplicationEventHandler) error,
	)
	NotifySystemCatalogReplicationEventHandler(
		fn func(handler eventhandlers.SystemCatalogReplicationEventHandler) error,
	)
	NotifyCompressionReplicationEventHandler(
		fn func(handler eventhandlers.CompressionReplicationEventHandler) error,
	)
	NotifyHypertableReplicationEventHandler(
		fn func(handler eventhandlers.HypertableReplicationEventHandler) error,
	)
	NotifyLogicalReplicationEventHandler(
		fn func(handler eventhandlers.LogicalReplicationEventHandler) error,
	)
	NotifySnapshottingEventHandler(
		fn func(handler eventhandlers.SnapshottingEventHandler) error,
	)
}
