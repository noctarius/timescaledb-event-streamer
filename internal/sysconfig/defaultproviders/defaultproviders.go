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
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/stream"
)

type EventEmitterProvider = func(
	config *spiconfig.Config, replicationContext context.ReplicationContext,
	streamManager stream.Manager, typeManager pgtypes.TypeManager,
) (*eventemitting.EventEmitter, error)

func DefaultSideChannelProvider(replicationContext context.ReplicationContext) (context.SideChannel, error) {
	return context.NewSideChannel(replicationContext)
}

func DefaultSinkManagerProvider(config *spiconfig.Config, stateStorageManager statestorage.Manager) (sink.Manager, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertySink, spiconfig.Stdout)
	s, err := sink.NewSink(name, config)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return sink.NewSinkManager(stateStorageManager, s), nil
}

func DefaultNamingStrategyProvider(config *spiconfig.Config) (namingstrategy.NamingStrategy, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertyNamingStrategy, spiconfig.Debezium)
	return namingstrategy.NewNamingStrategy(name, config)
}

func DefaultEventEmitterProvider(
	config *spiconfig.Config, replicationContext context.ReplicationContext,
	streamManager stream.Manager, typeManager pgtypes.TypeManager,
) (*eventemitting.EventEmitter, error) {

	filters, err := eventfiltering.NewEventFilter(config.Sink.Filters)
	if err != nil {
		return nil, err
	}

	return eventemitting.NewEventEmitter(replicationContext, streamManager, typeManager, filters)
}

func DefaultStateStorageManagerProvider(config *spiconfig.Config) (statestorage.Manager, error) {
	name := spiconfig.GetOrDefault(config, spiconfig.PropertyStateStorageType, spiconfig.NoneStorage)
	s, err := statestorage.NewStateStorage(name, config)
	if err != nil {
		return nil, err
	}
	return statestorage.NewStateStorageManager(s), nil
}
func DefaultReplicationContextProvider(
	config *spiconfig.Config, pgxConfig *pgx.ConnConfig,
	stateStorageManager statestorage.Manager,
	sideChannelProvider context.SideChannelProvider,
) (context.ReplicationContext, error) {

	return context.NewReplicationContext(config, pgxConfig, stateStorageManager, sideChannelProvider)
}

func DefaultStreamManagerProvider(
	nameGenerator schema.NameGenerator, typeManager pgtypes.TypeManager, sinkManager sink.Manager,
) (stream.Manager, error) {

	return stream.NewStreamManager(nameGenerator, typeManager, sinkManager)
}
