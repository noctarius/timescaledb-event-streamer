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

package sink

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"sync"
)

var sinkRegistry *registry

func init() {
	sinkRegistry = &registry{
		mutex:     sync.Mutex{},
		providers: make(map[config.SinkType]Provider),
	}
}

type registry struct {
	mutex     sync.Mutex
	providers map[config.SinkType]Provider
}

// RegisterSink registers a config.SinkType to a Provider
// implementation which creates the Sink when requested
func RegisterSink(name config.SinkType, provider Provider) bool {
	sinkRegistry.mutex.Lock()
	defer sinkRegistry.mutex.Unlock()
	if _, present := sinkRegistry.providers[name]; !present {
		sinkRegistry.providers[name] = provider
		return true
	}
	return false
}

// NewSink instantiates a new instance of the requested
// Sink when available, otherwise returns an error.
func NewSink(name config.SinkType, config *config.Config) (Sink, error) {
	sinkRegistry.mutex.Lock()
	defer sinkRegistry.mutex.Unlock()
	if p, present := sinkRegistry.providers[name]; present {
		return p(config)
	}
	return nil, errors.Errorf("SinkType '%s' doesn't exist", name)
}
