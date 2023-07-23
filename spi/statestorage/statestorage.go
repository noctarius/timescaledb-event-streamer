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
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
)

type Factory = func(config *config.Config) (Storage, error)

type Provider = func(config *config.Config) (Manager, error)

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
