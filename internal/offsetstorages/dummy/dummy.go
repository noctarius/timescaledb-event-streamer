package dummy

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/offset"
)

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
