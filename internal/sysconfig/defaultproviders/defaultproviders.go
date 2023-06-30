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

package defaultproviders

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

type EventEmitterProvider = func(
	config *spiconfig.Config, replicationContext *context.ReplicationContext, sink sink.Sink,
) (*eventemitting.EventEmitter, error)

func DefaultSinkProvider(config *spiconfig.Config) (sink.Sink, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertySink, spiconfig.Stdout)
	return sink.NewSink(name, config)
}

func DefaultNamingStrategyProvider(config *spiconfig.Config) (namingstrategy.NamingStrategy, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertyNamingStrategy, spiconfig.Debezium)
	return namingstrategy.NewNamingStrategy(name, config)
}

func DefaultEventEmitterProvider(
	config *spiconfig.Config, replicationContext *context.ReplicationContext, sink sink.Sink,
) (*eventemitting.EventEmitter, error) {

	filters, err := eventfiltering.NewEventFilter(config.Sink.Filters)
	if err != nil {
		return nil, err
	}

	return eventemitting.NewEventEmitter(replicationContext, sink, filters)
}

func DefaultStateStorageProvider(config *spiconfig.Config) (statestorage.Storage, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertyStateStorageType, spiconfig.NoneStorage)
	return statestorage.NewStateStorage(name, config)
}
