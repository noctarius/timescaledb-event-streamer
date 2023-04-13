package logicalreplicationresolver

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/eventhandler"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"time"
)

func NewTransactionResolver(config *sysconfig.SystemConfig, dispatcher *eventhandler.Dispatcher,
	systemCatalog *systemcatalog.SystemCatalog) eventhandlers.BaseReplicationEventHandler {

	enabled := spiconfig.GetOrDefault(
		config.Config, "postgresql.transaction.window.enabled", true,
	)
	timeout := spiconfig.GetOrDefault(
		config.Config, "postgresql.transaction.window.timeout", time.Duration(60),
	) * time.Second
	maxSize := spiconfig.GetOrDefault(
		config.Config, "postgresql.transaction.window.maxsize", uint(10000),
	)

	if enabled && maxSize > 0 {
		return newTransactionTracker(timeout, maxSize, config, dispatcher, systemCatalog)
	}
	return newLogicalReplicationResolver(config, dispatcher, systemCatalog)
}
