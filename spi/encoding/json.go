/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package encoding

import (
	"github.com/goccy/go-json"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
)

type JsonEncoder struct {
	marshallerFunction func(value any) ([]byte, error)
}

func NewJsonEncoderWithConfig(
	c *config.Config,
) *JsonEncoder {

	customReflection := config.GetOrDefault(c, config.PropertyEncodingCustomReflection, true)
	return NewJsonEncoder(customReflection)
}

func NewJsonEncoder(
	customReflection bool,
) *JsonEncoder {

	marshallerFunction := json.Marshal
	if customReflection {
		marshallerFunction = json.MarshalNoEscape
	}

	return &JsonEncoder{
		marshallerFunction: marshallerFunction,
	}
}

func (j *JsonEncoder) Marshal(
	value any,
) ([]byte, error) {

	return j.marshallerFunction(value)
}

type JsonDecoder struct {
	unmarshallerFunction func(data []byte, v any) error
}

func NewJsonDecoderWithConfig(
	c *config.Config,
) *JsonDecoder {

	customReflection := config.GetOrDefault(c, config.PropertyEncodingCustomReflection, true)
	return NewJsonDecoder(customReflection)
}

func NewJsonDecoder(
	customReflection bool,
) *JsonDecoder {

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

func (j *JsonDecoder) Unmarshal(
	data []byte, v any,
) error {

	return j.unmarshallerFunction(data, v)
}
