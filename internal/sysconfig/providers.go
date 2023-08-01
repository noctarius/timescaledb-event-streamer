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

package sysconfig

import (
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/publicationmanager"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationchannel"
	replicationcontextimpl "github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationcontext"
	sidechannelimpl "github.com/noctarius/timescaledb-event-streamer/internal/sidechannel"
	systemcatalogimpl "github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	taskmanagerimpl "github.com/noctarius/timescaledb-event-streamer/internal/taskmanager"
	"github.com/noctarius/timescaledb-event-streamer/internal/typemanager"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/publication"
	"github.com/noctarius/timescaledb-event-streamer/spi/replicationcontext"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/stream"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
)

var (
	_ = TypeManagerProvider(typemanager.NewTypeManager)
	_ = SinkManagerProvider(sinkimpl.NewSinkManager)
	_ = SnapshotterProvider(snapshotting.NewSnapshotterFromConfig)
	_ = ReplicationChannelProvider(replicationchannel.NewReplicationChannel)
	_ = NameGeneratorProvider(schema.NewNameGeneratorFromConfig)
	_ = SideChannelProvider(sidechannelimpl.NewSideChannel)
	_ = StateStorageManagerProvider(statestorage.NewStateStorageManager)
	_ = ReplicationContextProvider(replicationcontextimpl.NewReplicationContext)
	_ = LogicalReplicationResolverProvider(logicalreplicationresolver.NewResolver)
	_ = StreamManagerProvider(stream.NewStreamManager)
	_ = SystemCatalogProvider(systemcatalogimpl.NewSystemCatalog)
	_ = EventEmitterProvider(eventemitting.NewEventEmitterFromConfig)
	_ = TaskManagerProvider(taskmanagerimpl.NewTaskManager)
	_ = PublicationManagerProvider(publicationmanager.NewPublicationManager)
)

type PublicationManagerProvider = func(
	*config.Config, sidechannel.SideChannel,
) publication.PublicationManager

type TaskManagerProvider = func(
	*config.Config,
) (task.TaskManager, error)

type TypeManagerProvider = func(
	sidechannel.SideChannel,
) (pgtypes.TypeManager, error)

type SinkManagerProvider = func(
	statestorage.Manager, sink.Sink,
) sink.Manager

type SnapshotterProvider = func(
	*config.Config, statestorage.Manager, sidechannel.SideChannel,
	task.TaskManager, publication.PublicationManager, pgtypes.TypeManager,
) (*snapshotting.Snapshotter, error)

type ReplicationChannelProvider = func(
	replicationcontext.ReplicationContext, pgtypes.TypeManager, task.TaskManager, publication.PublicationManager,
) (*replicationchannel.ReplicationChannel, error)

type NameGeneratorProvider = func(
	*config.Config, namingstrategy.NamingStrategy,
) schema.NameGenerator

type SideChannelProvider = func(
	statestorage.Manager, *pgx.ConnConfig,
) (sidechannel.SideChannel, error)

type StateStorageManagerProvider = func(
	statestorage.Storage,
) statestorage.Manager

type ReplicationContextProvider func(
	*config.Config, *pgx.ConnConfig, statestorage.Manager, sidechannel.SideChannel,
) (replicationcontext.ReplicationContext, error)

type LogicalReplicationResolverProvider = func(
	*config.Config, replicationcontext.ReplicationContext,
	systemcatalog.SystemCatalog, pgtypes.TypeManager, task.TaskManager,
) (eventhandlers.BaseReplicationEventHandler, error)

type StreamManagerProvider = func(
	schema.NameGenerator, pgtypes.TypeManager, sink.Manager,
) (stream.Manager, error)

type SystemCatalogProvider = func(
	*config.Config, *pgx.ConnConfig, sidechannel.SideChannel, pgtypes.TypeManager,
	*snapshotting.Snapshotter, task.TaskManager, publication.PublicationManager, statestorage.Manager,
) (systemcatalog.SystemCatalog, error)

type EventEmitterProvider = func(
	*config.Config, replicationcontext.ReplicationContext, stream.Manager, pgtypes.TypeManager, task.TaskManager,
) (*eventemitting.EventEmitter, error)
