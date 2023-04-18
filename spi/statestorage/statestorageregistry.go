package statestorage

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

var stateStorageRegistry *Registry

func init() {
	stateStorageRegistry = &Registry{
		mutex:     sync.Mutex{},
		providers: make(map[config.StateStorageType]Provider),
	}
}

type Registry struct {
	mutex     sync.Mutex
	providers map[config.StateStorageType]Provider
}

func RegisterStateStorage(name config.StateStorageType, provider Provider) bool {
	stateStorageRegistry.mutex.Lock()
	defer stateStorageRegistry.mutex.Unlock()
	if _, present := stateStorageRegistry.providers[name]; !present {
		stateStorageRegistry.providers[name] = provider
		return true
	}
	return false
}
func NewStateStorage(name config.StateStorageType, config *config.Config) (Storage, error) {
	stateStorageRegistry.mutex.Lock()
	defer stateStorageRegistry.mutex.Unlock()
	if p, present := stateStorageRegistry.providers[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("StateStorageType '%s' doesn't exist", name)
}
