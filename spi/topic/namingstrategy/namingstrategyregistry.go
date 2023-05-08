package namingstrategy

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

var namingStrategyRegistry *registry

func init() {
	namingStrategyRegistry = &registry{
		mutex:     sync.Mutex{},
		providers: make(map[config.NamingStrategyType]Provider),
	}
}

type registry struct {
	mutex     sync.Mutex
	providers map[config.NamingStrategyType]Provider
}

// RegisterNamingStrategy registers a NamingRegistryType to a
// Provider implementation which creates the NamingStrategy
// when requested
func RegisterNamingStrategy(name config.NamingStrategyType, provider Provider) bool {
	namingStrategyRegistry.mutex.Lock()
	defer namingStrategyRegistry.mutex.Unlock()
	if _, present := namingStrategyRegistry.providers[name]; !present {
		namingStrategyRegistry.providers[name] = provider
		return true
	}
	return false
}

// NewNamingStrategy instantiates a new instance of the requested
// NamingStrategy when available, otherwise returns an error.
func NewNamingStrategy(name config.NamingStrategyType, config *config.Config) (NamingStrategy, error) {
	namingStrategyRegistry.mutex.Lock()
	defer namingStrategyRegistry.mutex.Unlock()
	if p, present := namingStrategyRegistry.providers[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("NamingStrategyType '%s' doesn't exist", name)
}
