//go:build !linux && !freebsd && !darwin

package plugins

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"runtime"
)

func LoadPlugins(
	config *config.Config,
) error {

	if len(config.Plugins) > 0 {
		return errors.Errorf("Plugins aren't supported on %s, but plugins are defined in config", runtime.GOOS)
	}
	return nil
}
