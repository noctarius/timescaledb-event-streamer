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
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

var stateStorageRegistry = &registry{
	mutex:     sync.Mutex{},
	factories: make(map[config.StateStorageType]Factory),
}

type registry struct {
	mutex     sync.Mutex
	factories map[config.StateStorageType]Factory
}

// RegisterStateStorage registers a config.StateStorageType to a
// Provider implementation which creates the Storage
// when requested
func RegisterStateStorage(name config.StateStorageType, factory Factory) bool {
	stateStorageRegistry.mutex.Lock()
	defer stateStorageRegistry.mutex.Unlock()
	if _, present := stateStorageRegistry.factories[name]; !present {
		stateStorageRegistry.factories[name] = factory
		return true
	}
	return false
}

// NewStateStorage instantiates a new instance of the requested
// Storage when available, otherwise returns an error.
func NewStateStorage(name config.StateStorageType, config *config.Config) (Storage, error) {
	stateStorageRegistry.mutex.Lock()
	defer stateStorageRegistry.mutex.Unlock()
	if p, present := stateStorageRegistry.factories[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("StateStorageType '%s' doesn't exist", name)
}
