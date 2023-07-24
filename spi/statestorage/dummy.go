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

package statestorage

import (
	"encoding"
	"github.com/go-errors/errors"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
)

func init() {
	RegisterStateStorage(spiconfig.NoneStorage, func(_ *spiconfig.Config) (Storage, error) {
		return NewDummyStateStorage(), nil
	})
}

type dummyStateStorage struct {
	offsets       map[string]*Offset
	encodedStates map[string][]byte
}

func NewDummyStateStorage() Storage {
	return &dummyStateStorage{
		offsets:       make(map[string]*Offset),
		encodedStates: make(map[string][]byte),
	}
}

func (d *dummyStateStorage) Start() error {
	return nil
}

func (d *dummyStateStorage) Stop() error {
	return nil
}

func (d *dummyStateStorage) Save() error {
	return nil
}

func (d *dummyStateStorage) Load() error {
	return nil
}

func (d *dummyStateStorage) Get() (map[string]*Offset, error) {
	return d.offsets, nil
}

func (d *dummyStateStorage) Set(
	key string, value *Offset,
) error {

	d.offsets[key] = value
	return nil
}

func (d *dummyStateStorage) StateEncoder(
	name string, encoder encoding.BinaryMarshaler,
) error {

	data, err := encoder.MarshalBinary()
	if err != nil {
		return err
	}
	d.encodedStates[name] = data
	return nil
}

func (d *dummyStateStorage) StateDecoder(
	name string, decoder encoding.BinaryUnmarshaler,
) (bool, error) {

	if data, present := d.encodedStates[name]; present {
		if err := decoder.UnmarshalBinary(data); err != nil {
			return true, errors.Wrap(err, 0)
		}
		return true, nil
	}
	return false, nil
}

func (d *dummyStateStorage) EncodedState(
	key string,
) (encodedState []byte, present bool) {

	encodedState, present = d.encodedStates[key]
	return
}

func (d *dummyStateStorage) SetEncodedState(
	key string, encodedState []byte,
) {

	d.encodedStates[key] = encodedState
}
