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

package dummy

import (
	"encoding"
	"github.com/go-errors/errors"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
)

func init() {
	statestorage.RegisterStateStorage(spiconfig.NoneStorage, func(_ *spiconfig.Config) (statestorage.Storage, error) {
		return NewDummyStateStorage(), nil
	})
}

type DummyStateStorage struct {
	offsets       map[string]*statestorage.Offset
	encodedStates map[string][]byte
}

func NewDummyStateStorage() *DummyStateStorage {
	return &DummyStateStorage{
		offsets:       make(map[string]*statestorage.Offset),
		encodedStates: make(map[string][]byte),
	}
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
	return d.offsets, nil
}

func (d *DummyStateStorage) Set(key string, value *statestorage.Offset) error {
	d.offsets[key] = value
	return nil
}

func (d *DummyStateStorage) StateEncoder(name string, encoder encoding.BinaryMarshaler) error {
	data, err := encoder.MarshalBinary()
	if err != nil {
		return err
	}
	d.encodedStates[name] = data
	return nil
}

func (d *DummyStateStorage) StateDecoder(name string, decoder encoding.BinaryUnmarshaler) (bool, error) {
	if data, present := d.encodedStates[name]; present {
		if err := decoder.UnmarshalBinary(data); err != nil {
			return true, errors.Wrap(err, 0)
		}
		return true, nil
	}
	return false, nil
}

func (d *DummyStateStorage) EncodedState(key string) (encodedState []byte, present bool) {
	encodedState, present = d.encodedStates[key]
	return
}

func (d *DummyStateStorage) SetEncodedState(key string, encodedState []byte) {
	d.encodedStates[key] = encodedState
}
