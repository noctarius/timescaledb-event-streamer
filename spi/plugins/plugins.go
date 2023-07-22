//go:build linux || freebsd || darwin

package plugins

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"plugin"
)

type ExtensionPoints interface {
	RegisterNamingStrategy(name string, provider namingstrategy.Provider) bool
	RegisterStateStorage(name string, provider statestorage.Provider) bool
	RegisterSink(name string, provider sink.Provider) bool
}

type PluginInitialize func(extensionPoints ExtensionPoints) error

func LoadPlugins(config *config.Config) error {
	for _, pluginPath := range config.Plugins {
		p, err := plugin.Open(pluginPath)
		if err != nil {
			return err
		}

		s, err := p.Lookup("PluginInitialize")
		if err != nil {
			return err
		}

		if err := s.(PluginInitialize)(&extensionPoints{}); err != nil {
			return err
		}
	}
	return nil
}

type extensionPoints struct {
}

func (*extensionPoints) RegisterNamingStrategy(name string, provider namingstrategy.Provider) bool {
	return namingstrategy.RegisterNamingStrategy(config.NamingStrategyType(name), provider)
}

func (*extensionPoints) RegisterStateStorage(name string, provider statestorage.Provider) bool {
	return statestorage.RegisterStateStorage(config.StateStorageType(name), provider)
}

func (*extensionPoints) RegisterSink(name string, provider sink.Provider) bool {
	return sink.RegisterSink(config.SinkType(name), provider)
}
