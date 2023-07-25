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
	"bytes"
	stderrors "errors"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/erroring"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventemitting"
	"github.com/noctarius/timescaledb-event-streamer/internal/functional"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationchannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	intsystemcatalog "github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/wiring"
	"github.com/samber/lo"
	"github.com/urfave/cli"
)

const esPreviouslyKnownChunks = "::previously::known::chunks"

// Replicator is the main controller for all things logical replication,
// such as the logical replication connection, the side channel connection,
// and other services necessary to run the event stream generation.
type Replicator struct {
	logger       *logging.Logger
	config       *sysconfig.SystemConfig
	shutdownTask func() error
}

// NewReplicator instantiates a new instance of the Replicator.
func NewReplicator(
	config *sysconfig.SystemConfig,
) (*Replicator, error) {

	logger, err := logging.NewLogger("Replicator")
	if err != nil {
		return nil, err
	}

	return &Replicator{
		logger: logger,
		config: config,
	}, nil
}

// StartReplication initiates the actual replication process
func (r *Replicator) StartReplication() *cli.ExitError {
	logger, err := logging.NewLogger("Replicator")
	if err != nil {
		return erroring.AdaptError(err, 1)
	}

	container, err := wiring.NewContainer(
		EventingModule,
		LogicalReplicationResolverModule,
		SchemaModule,
		NamingStrategyModule,
		ReplicationChannelModule,
		ReplicationContextModule,
		SideChannelModule,
		SinkManagerModule,
		StateStorageModule,
		StreamManagerModule,
		SystemCatalogModule,
		TypeManagerModule,
		SnapshotterModule,
		wiring.DefineModule("Config", func(module wiring.Module) {
			module.Provide(func() *config.Config {
				return r.config.Config
			})
			module.Provide(func() *pgx.ConnConfig {
				return r.config.PgxConfig
			})
			module.Invoke(func(replicationContext context.ReplicationContext, typeManager pgtypes.TypeManager) error {
				// Check version information
				if !replicationContext.IsMinimumPostgresVersion() {
					return cli.NewExitError("timescaledb-event-streamer requires PostgreSQL 13 or later", 11)
				}
				if !replicationContext.IsMinimumTimescaleVersion() {
					return cli.NewExitError("timescaledb-event-streamer requires TimescaleDB 2.10 or later", 12)
				}

				// Check WAL replication level
				if !replicationContext.IsLogicalReplicationEnabled() {
					return cli.NewExitError("timescaledb-event-streamer requires wal_level set to 'logical'", 16)
				}

				// Log system information
				logger.Infof("Discovered System Information:")
				logger.Infof("  * PostgreSQL version %s", replicationContext.PostgresVersion())
				logger.Infof("  * TimescaleDB version %s", replicationContext.TimescaleVersion())
				logger.Infof("  * PostgreSQL System Identity %s", replicationContext.SystemId())
				logger.Infof("  * PostgreSQL Timeline %d", replicationContext.Timeline())
				logger.Infof("  * PostgreSQL DatabaseName %s", replicationContext.DatabaseName())
				logger.Infof("  * PostgreSQL Types loaded %d", typeManager.NumKnownTypes())
				return nil
			})
		}),
		wiring.DefineModule("Overrides", func(module wiring.Module) {
			module.MayProvide(r.config.EventEmitterProvider)
			module.MayProvide(r.config.LogicalReplicationResolverProvider)
			module.MayProvide(r.config.NameGeneratorProvider)
			module.MayProvide(r.config.NamingStrategyProvider)
			module.MayProvide(r.config.ReplicationChannelProvider)
			module.MayProvide(r.config.ReplicationContextProvider)
			module.MayProvide(r.config.SideChannelProvider)
			module.MayProvide(r.config.SinkManagerProvider)
			module.MayProvide(r.config.SnapshotterProvider)
			module.MayProvide(r.config.StateStorageManagerProvider)
			module.MayProvide(r.config.StateStorageProvider)
			module.MayProvide(r.config.StreamManagerProvider)
			module.MayProvide(r.config.SystemCatalogProvider)
			module.MayProvide(r.config.TypeManagerProvider)
		}),
	)
	if err != nil {
		return erroring.AdaptError(err, 1)
	}

	var replicationContext context.ReplicationContext
	if err := container.Service(&replicationContext); err != nil {
		return erroring.AdaptError(err, 1)
	}

	// Start internal dispatching
	if err := replicationContext.StartReplicationContext(); err != nil {
		return erroring.AdaptErrorWithMessage(err, "failed to start replication context", 18)
	}

	// Start event emitter
	var eventEmitter *eventemitting.EventEmitter
	if err := container.Service(&eventEmitter); err != nil {
		return erroring.AdaptError(err, 1)
	}

	if err := eventEmitter.Start(); err != nil {
		return erroring.AdaptErrorWithMessage(err, "failed to start event emitter", 24)
	}

	// Start the snapshotter
	var snapshotter *snapshotting.Snapshotter
	if err := container.Service(&snapshotter); err != nil {
		return erroring.AdaptError(err, 1)
	}
	snapshotter.StartSnapshotter()

	// Get initial list of chunks to add to
	var systemCatalog *intsystemcatalog.SystemCatalog
	if err := container.Service(&systemCatalog); err != nil {
		return erroring.AdaptError(err, 1)
	}
	initialChunkTables, err := r.collectChunksForPublication(
		replicationContext.StateStorageManager().EncodedState, systemCatalog.GetAllChunks,
		replicationContext.PublicationManager().ReadPublishedTables,
	)
	if err != nil {
		return erroring.AdaptErrorWithMessage(err, "failed to read known chunks", 25)
	}

	var replicationChannel *replicationchannel.ReplicationChannel
	if err := container.Service(&replicationChannel); err != nil {
		return erroring.AdaptError(err, 1)
	}
	if err := replicationChannel.StartReplicationChannel(replicationContext, initialChunkTables); err != nil {
		return erroring.AdaptError(err, 16)
	}

	r.shutdownTask = func() error {
		snapshotter.StopSnapshotter()
		err1 := replicationChannel.StopReplicationChannel()
		err2 := eventEmitter.Stop()
		state, err3 := encodeKnownChunks(systemCatalog.GetAllChunks())
		if err3 == nil {
			replicationContext.StateStorageManager().SetEncodedState(esPreviouslyKnownChunks, state)
		}
		err4 := replicationContext.StopReplicationContext()
		return stderrors.Join(err1, err2, err3, err4)
	}

	return nil
}

// StopReplication initiates a clean shutdown of the replication process. This
// call blocks until the shutdown process has finished.
func (r *Replicator) StopReplication() *cli.ExitError {
	if r.shutdownTask != nil {
		return erroring.AdaptError(r.shutdownTask(), 250)
	}
	return nil
}

func (r *Replicator) collectChunksForPublication(
	encodedState func(name string) ([]byte, bool),
	getAllChunks func() []systemcatalog.SystemEntity,
	readPublishedTables func() ([]systemcatalog.SystemEntity, error),
) ([]systemcatalog.SystemEntity, error) {

	// Get initial list of chunks to add to publication
	allKnownTables, err := getKnownChunks(encodedState, getAllChunks)
	if err != nil {
		return nil, erroring.AdaptErrorWithMessage(err, "failed to read known chunks", 25)
	}

	r.logger.Debugf(
		"All interesting chunks: %+v",
		lo.Map(allKnownTables, functional.MappingTransformer(systemcatalog.SystemEntity.CanonicalName)),
	)

	// Filter published chunks to only add new chunks
	alreadyPublished, err := readPublishedTables()
	if err != nil {
		return nil, erroring.AdaptError(err, 250)
	}
	alreadyPublished = lo.Filter(alreadyPublished, func(item systemcatalog.SystemEntity, _ int) bool {
		return item.SchemaName() == "_timescaledb_internal"
	})

	r.logger.Debugf(
		"Chunks already in publication: %+v",
		lo.Map(alreadyPublished, functional.MappingTransformer(systemcatalog.SystemEntity.CanonicalName)),
	)

	initialChunkTables := lo.Filter(allKnownTables, func(item systemcatalog.SystemEntity, _ int) bool {
		return !lo.ContainsBy(alreadyPublished, func(other systemcatalog.SystemEntity) bool {
			return item.CanonicalName() == other.CanonicalName()
		})
	})
	r.logger.Debugf(
		"Chunks to be added publication: %+v",
		lo.Map(initialChunkTables, functional.MappingTransformer(systemcatalog.SystemEntity.CanonicalName)),
	)
	return initialChunkTables, nil
}

func getKnownChunks(
	encodedState func(name string) ([]byte, bool),
	getAllChunks func() []systemcatalog.SystemEntity,
) ([]systemcatalog.SystemEntity, error) {

	allChunks := getAllChunks()
	if state, present := encodedState(esPreviouslyKnownChunks); present {
		candidates, err := decodeKnownChunks(state)
		if err != nil {
			return nil, err
		}

		// Filter potentially deleted chunks
		return lo.Filter(candidates, func(item systemcatalog.SystemEntity, index int) bool {
			return lo.ContainsBy(allChunks, func(other systemcatalog.SystemEntity) bool {
				return item.CanonicalName() == other.CanonicalName()
			})
		}), nil
	}

	return allChunks, nil
}

func decodeKnownChunks(
	data []byte,
) ([]systemcatalog.SystemEntity, error) {

	buffer := encoding.NewReadBuffer(bytes.NewBuffer(data))

	numOfChunks, err := buffer.ReadUint32()
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	chunks := make([]systemcatalog.SystemEntity, 0, numOfChunks)
	for i := 0; i < int(numOfChunks); i++ {
		schemaName, err := buffer.ReadString()
		if err != nil {
			return nil, errors.Wrap(err, 0)
		}
		tableName, err := buffer.ReadString()
		if err != nil {
			return nil, errors.Wrap(err, 0)
		}
		chunks = append(chunks, systemcatalog.NewSystemEntity(schemaName, tableName))
	}
	return chunks, nil
}

func encodeKnownChunks(
	chunks []systemcatalog.SystemEntity,
) ([]byte, error) {

	buffer := encoding.NewWriteBuffer(1024)

	numOfChunks := len(chunks)
	if err := buffer.PutUint32(uint32(numOfChunks)); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	for i := 0; i < numOfChunks; i++ {
		chunk := chunks[i]
		if err := buffer.PutString(chunk.SchemaName()); err != nil {
			return nil, errors.Wrap(err, 0)
		}
		if err := buffer.PutString(chunk.TableName()); err != nil {
			return nil, errors.Wrap(err, 0)
		}
	}
	return buffer.Bytes(), nil
}
