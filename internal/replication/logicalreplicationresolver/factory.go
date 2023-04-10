package logicalreplicationresolver

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventhandler"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"time"
)

func NewTransactionResolver(config *sysconfig.SystemConfig, dispatcher *eventhandler.Dispatcher,
	systemCatalog *systemcatalog.SystemCatalog) eventhandler.BaseReplicationEventHandler {

	enabled := configuring.GetOrDefault(
		config.Config, "postgresql.transaction.window.enabled", true,
	)
	timeout := configuring.GetOrDefault(
		config.Config, "postgresql.transaction.window.timeout", time.Duration(60),
	) * time.Second
	maxSize := configuring.GetOrDefault(
		config.Config, "postgresql.transaction.window.maxsize", uint(10000),
	)

	if enabled && maxSize > 0 {
		return newTransactionTracker(timeout, maxSize, config, dispatcher, systemCatalog)
	}
	return newLogicalReplicationResolver(config, dispatcher, systemCatalog)
}
