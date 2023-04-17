package dummy

import (
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/offset"
)

func init() {
	offset.RegisterOffsetStorage(spiconfig.NoneStorage, func(_ *spiconfig.Config) (offset.Storage, error) {
		return &DummyOffsetStorage{}, nil
	})
}

type DummyOffsetStorage struct {
}

func (d *DummyOffsetStorage) Start() error {
	return nil
}

func (d *DummyOffsetStorage) Stop() error {
	return nil
}

func (d *DummyOffsetStorage) Save() error {
	return nil
}

func (d *DummyOffsetStorage) Load() error {
	return nil
}

func (d *DummyOffsetStorage) Get() (map[string]*offset.Offset, error) {
	return nil, nil
}

func (d *DummyOffsetStorage) Set(key string, value *offset.Offset) error {
	return nil
}
