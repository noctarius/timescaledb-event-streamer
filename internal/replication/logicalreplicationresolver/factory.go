package logicalreplicationresolver

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"time"
)

func NewResolver(config *spiconfig.Config, replicationContext *context.ReplicationContext,
	systemCatalog *systemcatalog.SystemCatalog) (eventhandlers.BaseReplicationEventHandler, error) {

	enabled := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlTxwindowEnabled, true,
	)
	timeout := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlTxwindowTimeout, time.Duration(60),
	) * time.Second
	maxSize := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlTxwindowMaxsize, uint(10000),
	)

	if enabled && maxSize > 0 {
		return newTransactionTracker(timeout, maxSize, config, replicationContext, systemCatalog)
	}
	return newLogicalReplicationResolver(config, replicationContext, systemCatalog)
}
