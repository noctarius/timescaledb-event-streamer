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

package namingstrategy

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

type Factory func(config *config.Config) (NamingStrategy, error)

var namingStrategyRegistry = &registry{
	mutex:     sync.Mutex{},
	factories: make(map[config.NamingStrategyType]Factory),
}

type registry struct {
	mutex     sync.Mutex
	factories map[config.NamingStrategyType]Factory
}

// RegisterNamingStrategy registers a NamingRegistryType to a
// Provider implementation which creates the NamingStrategy
// when requested
func RegisterNamingStrategy(
	name config.NamingStrategyType, factory Factory,
) bool {

	namingStrategyRegistry.mutex.Lock()
	defer namingStrategyRegistry.mutex.Unlock()
	if _, present := namingStrategyRegistry.factories[name]; !present {
		namingStrategyRegistry.factories[name] = factory
		return true
	}
	return false
}

// NewNamingStrategy instantiates a new instance of the requested
// NamingStrategy when available, otherwise returns an error.
func NewNamingStrategy(
	name config.NamingStrategyType, config *config.Config,
) (NamingStrategy, error) {

	namingStrategyRegistry.mutex.Lock()
	defer namingStrategyRegistry.mutex.Unlock()
	if p, present := namingStrategyRegistry.factories[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("NamingStrategyType '%s' doesn't exist", name)
}
