package encoding

import (
	"github.com/goccy/go-json"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
)

type JsonEncoder struct {
	marshallerFunction func(value any) ([]byte, error)
}

func NewJsonEncoderWithConfig(c *config.Config) *JsonEncoder {
	customReflection := config.GetOrDefault(c, config.PropertyEncodingCustomReflection, true)
	return NewJsonEncoder(customReflection)
}

func NewJsonEncoder(customReflection bool) *JsonEncoder {
	marshallerFunction := json.Marshal
	if customReflection {
		marshallerFunction = json.MarshalNoEscape
	}

	return &JsonEncoder{
		marshallerFunction: marshallerFunction,
	}
}

func (j *JsonEncoder) Marshal(value any) ([]byte, error) {
	return j.marshallerFunction(value)
}

type JsonDecoder struct {
	unmarshallerFunction func(data []byte, v any) error
}

func NewJsonDecoderWithConfig(c *config.Config) *JsonDecoder {
	customReflection := config.GetOrDefault(c, config.PropertyEncodingCustomReflection, true)
	return NewJsonDecoder(customReflection)
}

func NewJsonDecoder(customReflection bool) *JsonDecoder {
	unmarshallerFunction := json.Unmarshal
	if customReflection {
		unmarshallerFunction = func(data []byte, v any) error {
			return json.UnmarshalNoEscape(data, v)
		}
	}

	return &JsonDecoder{
		unmarshallerFunction: unmarshallerFunction,
	}
}

func (j *JsonDecoder) Unmarshal(data []byte, v any) error {
	return j.unmarshallerFunction(data, v)
}
