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

package replication

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationchannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/stream"
	"github.com/noctarius/timescaledb-event-streamer/spi/wiring"
)

var StaticModule = wiring.DefineModule(
	"Static", func(module wiring.Module) {
		module.Provide(eventemitting.NewEventEmitterFromConfig)
		module.Provide(statestorage.NewStateStorageManager)
		module.Provide(sidechannel.NewSideChannel)
		module.Provide(replicationcontext.NewReplicationContext)
		module.Provide(logicalreplicationresolver.NewResolver, wiring.ForceInitialization())
		module.Provide(schema.NewNameGeneratorFromConfig)
		module.Provide(replicationchannel.NewReplicationChannel)
		module.Provide(sink.NewSinkManager)
		module.Provide(stream.NewStreamManager)
		module.Provide(snapshotting.NewSnapshotterFromConfig)

		module.Provide(func(sideChannel sidechannel.SideChannel) (pgtypes.TypeManager, error) {
			// Necessary since TypeManager is looking for TypeResolver,
			// not SideChannel (which implements the interface)
			return pgtypes.NewTypeManager(sideChannel)
		})

		module.Provide(func(
			c *config.Config, replicationContext replicationcontext.ReplicationContext,
			typeManager pgtypes.TypeManager, snapshotter *snapshotting.Snapshotter,
		) (*systemcatalog.SystemCatalog, error) {

			return systemcatalog.NewSystemCatalog(c, replicationContext, typeManager.ResolveDataType, snapshotter)
		})
	},
)

var DynamicModule = wiring.DefineModule(
	"Dynmic",
	func(module wiring.Module) {
		module.Provide(func(c *config.Config) (statestorage.Storage, error) {
			name := config.GetOrDefault(c, config.PropertyStateStorageType, config.NoneStorage)
			return statestorage.NewStateStorage(name, c)
		})

		module.Provide(func(c *config.Config) (namingstrategy.NamingStrategy, error) {
			name := config.GetOrDefault(c, config.PropertyNamingStrategy, config.Debezium)
			return namingstrategy.NewNamingStrategy(name, c)
		})

		module.Provide(func(c *config.Config) (sink.Sink, error) {
			name := config.GetOrDefault(c, config.PropertySink, config.Stdout)
			return sink.NewSink(name, c)
		})
	},
)
