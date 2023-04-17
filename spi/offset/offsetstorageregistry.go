package offset

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

var offsetStorageRegistry *Registry

func init() {
	offsetStorageRegistry = &Registry{
		mutex:     sync.Mutex{},
		providers: make(map[config.OffsetStorageType]Provider),
	}
}

type Registry struct {
	mutex     sync.Mutex
	providers map[config.OffsetStorageType]Provider
}

func RegisterOffsetStorage(name config.OffsetStorageType, provider Provider) bool {
	offsetStorageRegistry.mutex.Lock()
	defer offsetStorageRegistry.mutex.Unlock()
	if _, present := offsetStorageRegistry.providers[name]; !present {
		offsetStorageRegistry.providers[name] = provider
		return true
	}
	return false
}
func NewOffsetStorage(name config.OffsetStorageType, config *config.Config) (Storage, error) {
	offsetStorageRegistry.mutex.Lock()
	defer offsetStorageRegistry.mutex.Unlock()
	if p, present := offsetStorageRegistry.providers[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("OffsetStorageType '%s' doesn't exist", name)
}
