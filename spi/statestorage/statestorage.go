package statestorage

import (
	"encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
)

type Provider = func(config *config.Config) (Storage, error)

type StateEncoderFunc func() (data []byte, err error)

func (sef StateEncoderFunc) MarshalBinary() (data []byte, err error) {
	return sef()
}

type Storage interface {
	Start() error
	Stop() error
	Save() error
	Load() error
	Get() (map[string]*Offset, error)
	Set(key string, value *Offset) error
	StateEncoder(name string, encoder encoding.BinaryMarshaler) error
	StateDecoder(name string, decoder encoding.BinaryUnmarshaler) (present bool, err error)
	EncodedState(name string) (encodedState []byte, present bool)
	SetEncodedState(name string, encodedState []byte)
}
