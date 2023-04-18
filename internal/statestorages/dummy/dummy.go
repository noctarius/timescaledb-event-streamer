package dummy

import (
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
)

func init() {
	statestorage.RegisterStateStorage(spiconfig.NoneStorage, func(_ *spiconfig.Config) (statestorage.Storage, error) {
		return &DummyStateStorage{}, nil
	})
}

type DummyStateStorage struct {
}

func (d *DummyStateStorage) Start() error {
	return nil
}

func (d *DummyStateStorage) Stop() error {
	return nil
}

func (d *DummyStateStorage) Save() error {
	return nil
}

func (d *DummyStateStorage) Load() error {
	return nil
}

func (d *DummyStateStorage) Get() (map[string]*statestorage.Offset, error) {
	return nil, nil
}

func (d *DummyStateStorage) Set(key string, value *statestorage.Offset) error {
	return nil
}

func (d *DummyStateStorage) RegisterStateEncoder(name string, encoder statestorage.StateEncoder) {
}

func (d *DummyStateStorage) EncodedState(name string) (encodedState []byte, present bool) {
	return
}

func (d *DummyStateStorage) SetEncodedState(name string, encodedState []byte) {
}
