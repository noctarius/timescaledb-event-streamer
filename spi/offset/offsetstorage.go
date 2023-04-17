package offset

import "github.com/noctarius/timescaledb-event-streamer/spi/config"

type Provider = func(config *config.Config) (Storage, error)

type Storage interface {
	Start() error
	Stop() error
	Save() error
	Load() error
	Get() (map[string]*Offset, error)
	Set(key string, value *Offset) error
}
