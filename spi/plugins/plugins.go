//go:build linux || freebsd || darwin

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

package plugins

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"plugin"
)

type ExtensionPoints interface {
	RegisterNamingStrategy(
		name string, factory namingstrategy.Factory,
	) bool
	RegisterStateStorage(
		name string, factory statestorage.StorageProvider,
	) bool
	RegisterSink(
		name string, factory sink.Factory,
	) bool
}

type PluginInitialize func(extensionPoints ExtensionPoints) error

func LoadPlugins(
	config *config.Config,
) error {

	for _, pluginPath := range config.Plugins {
		p, err := plugin.Open(pluginPath)
		if err != nil {
			return err
		}

		s, err := p.Lookup("PluginInitialize")
		if err != nil {
			return err
		}

		if err := s.(PluginInitialize)(&extensionPoints{}); err != nil {
			return err
		}
	}
	return nil
}

type extensionPoints struct {
}

func (*extensionPoints) RegisterNamingStrategy(
	name string, factory namingstrategy.Factory,
) bool {

	return namingstrategy.RegisterNamingStrategy(config.NamingStrategyType(name), factory)
}

func (*extensionPoints) RegisterStateStorage(
	name string, factory statestorage.StorageProvider,
) bool {

	return statestorage.RegisterStateStorage(config.StateStorageType(name), factory)
}

func (*extensionPoints) RegisterSink(
	name string, factory sink.Factory,
) bool {

	return sink.RegisterSink(config.SinkType(name), factory)
}
