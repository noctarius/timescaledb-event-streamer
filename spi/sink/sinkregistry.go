package sink

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

var sinkRegistry *Registry

func init() {
	sinkRegistry = &Registry{
		mutex:     sync.Mutex{},
		providers: make(map[config.SinkType]Provider),
	}
}

type Registry struct {
	mutex     sync.Mutex
	providers map[config.SinkType]Provider
}

func RegisterSink(name config.SinkType, provider Provider) bool {
	sinkRegistry.mutex.Lock()
	defer sinkRegistry.mutex.Unlock()
	if _, present := sinkRegistry.providers[name]; !present {
		sinkRegistry.providers[name] = provider
		return true
	}
	return false
}

func NewSink(name config.SinkType, config *config.Config) (Sink, error) {
	sinkRegistry.mutex.Lock()
	defer sinkRegistry.mutex.Unlock()
	if p, present := sinkRegistry.providers[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("SinkType '%s' doesn't exist", name)
}
