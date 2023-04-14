package sysconfig

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig/defaultproviders"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

type SystemConfig struct {
	*spiconfig.Config

	PgxConfig              *pgx.ConnConfig
	SinkProvider           sink.Provider
	EventEmitterProvider   defaultproviders.EventEmitterProvider
	NamingStrategyProvider namingstrategy.Provider
}

func NewSystemConfig(config *spiconfig.Config) *SystemConfig {
	sc := &SystemConfig{
		Config: config,
	}
	sc.SinkProvider = defaultproviders.DefaultSinkProvider
	sc.EventEmitterProvider = defaultproviders.DefaultEventEmitterProvider
	sc.NamingStrategyProvider = defaultproviders.DefaultNamingStrategyProvider
	return sc
}
