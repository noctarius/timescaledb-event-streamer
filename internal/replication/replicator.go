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
	"fmt"
	"github.com/go-errors/errors"
	logrepresolver "github.com/noctarius/timescaledb-event-streamer/internal/replication/logicalreplicationresolver"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/replicationchannel"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	intsystemcatalog "github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
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
func NewReplicator(config *sysconfig.SystemConfig) (*Replicator, error) {
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
	namingStrategy, err := r.config.NamingStrategyProvider(r.config.Config)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate naming strategy", 21)
	}
	nameGenerator := schema.NewNameGenerator(r.config.Topic.Prefix, namingStrategy)

	stateStorageManager, err := r.config.StateStorageManagerProvider(r.config.Config)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate state storage", 23)
	}

	// Create the side channels and replication context
	replicationContext, err := r.config.ReplicationContextProvider(
		r.config.Config, r.config.PgxConfig, stateStorageManager, r.config.SideChannelProvider,
	)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to initialize replication context", 17)
	}

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

	// Instantiate the type manager
	typeManager, err := pgtypes.NewTypeManager(replicationContext.TypeResolver())
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate type manager", 26)
	}

	// Log system information
	r.logger.Infof("Discovered System Information:")
	r.logger.Infof("  * PostgreSQL version %s", replicationContext.PostgresVersion())
	r.logger.Infof("  * TimescaleDB version %s", replicationContext.TimescaleVersion())
	r.logger.Infof("  * PostgreSQL System Identity %s", replicationContext.SystemId())
	r.logger.Infof("  * PostgreSQL Timeline %d", replicationContext.Timeline())
	r.logger.Infof("  * PostgreSQL DatabaseName %s", replicationContext.DatabaseName())
	r.logger.Infof("  * PostgreSQL Types loaded %d", typeManager.NumKnownTypes())

	// Create replication channel and internal replication handler
	replicationChannel, err := replicationchannel.NewReplicationChannel(replicationContext)
	if err != nil {
		return supporting.AdaptError(err, 18)
	}

	// Instantiate the snapshotter
	snapshotter, err := snapshotting.NewSnapshotter(5, replicationContext)
	if err != nil {
		return supporting.AdaptError(err, 19)
	}

	// Instantiate the sink manager
	sinkManager, err := r.config.SinkManagerProvider(r.config.Config, stateStorageManager)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate sink manager", 22)
	}

	// Instantiate the stream manager
	streamManager, err := r.config.StreamManagerProvider(nameGenerator, typeManager, sinkManager)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to instantiate stream manager", 25)
	}

	// Instantiate the change event emitter
	eventEmitter, err := r.config.EventEmitterProvider(r.config.Config, replicationContext, streamManager, typeManager)
	if err != nil {
		return supporting.AdaptError(err, 14)
	}

	// Set up the system catalog (replicating the TimescaleDB internal representation)
	systemCatalog, err := intsystemcatalog.NewSystemCatalog(
		r.config.Config, replicationContext, typeManager.DataType, snapshotter,
	)
	if err != nil {
		return supporting.AdaptError(err, 15)
	}

	// Set up the internal transaction tracking and logical replication resolving
	transactionResolver, err := logrepresolver.NewResolver(r.config.Config, replicationContext, systemCatalog)
	if err != nil {
		return supporting.AdaptError(err, 20)
	}

	// Register event handlers
	taskManager := replicationContext.TaskManager()
	taskManager.RegisterReplicationEventHandler(transactionResolver)
	taskManager.RegisterReplicationEventHandler(systemCatalog.NewEventHandler())
	taskManager.RegisterReplicationEventHandler(eventEmitter.NewEventHandler())

	// Start internal dispatching
	if err := replicationContext.StartReplicationContext(); err != nil {
		return cli.NewExitError(fmt.Sprintf("failed to start replication context: %s", err.Error()), 18)
	}

	// Start event emitter
	if err := eventEmitter.Start(); err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to start event emitter", 24)
	}

	// Start the snapshotter
	snapshotter.StartSnapshotter()

	// Get initial list of chunks to add to
	initialChunkTables, err := r.collectChunksForPublication(
		replicationContext.StateStorageManager().EncodedState, systemCatalog.GetAllChunks,
		replicationContext.PublicationManager().ReadPublishedTables,
	)
	if err != nil {
		return supporting.AdaptErrorWithMessage(err, "failed to read known chunks", 25)
	}

	if err := replicationChannel.StartReplicationChannel(replicationContext, initialChunkTables); err != nil {
		return supporting.AdaptError(err, 16)
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
		return supporting.AdaptError(r.shutdownTask(), 250)
	}
	return nil
}

func (r *Replicator) collectChunksForPublication(encodedState func(name string) ([]byte, bool),
	getAllChunks func() []systemcatalog.SystemEntity, readPublishedTables func() ([]systemcatalog.SystemEntity, error),
) ([]systemcatalog.SystemEntity, error) {

	// Get initial list of chunks to add to publication
	allKnownTables, err := getKnownChunks(encodedState, getAllChunks)
	if err != nil {
		return nil, supporting.AdaptErrorWithMessage(err, "failed to read known chunks", 25)
	}

	r.logger.Debugf(
		"All interesting chunks: %+v",
		supporting.Map(allKnownTables, func(chunk systemcatalog.SystemEntity) string {
			return chunk.TableName()
		}),
	)

	// Filter published chunks to only add new chunks
	alreadyPublished, err := readPublishedTables()
	if err != nil {
		return nil, supporting.AdaptError(err, 250)
	}
	alreadyPublished = supporting.Filter(alreadyPublished, func(item systemcatalog.SystemEntity) bool {
		return item.SchemaName() == "_timescaledb_internal"
	})

	r.logger.Debugf(
		"Chunks already in publication: %+v",
		supporting.Map(alreadyPublished, func(chunk systemcatalog.SystemEntity) string {
			return chunk.TableName()
		}),
	)

	initialChunkTables := supporting.Filter(allKnownTables, func(item systemcatalog.SystemEntity) bool {
		return !supporting.ContainsWithMatcher(alreadyPublished, func(other systemcatalog.SystemEntity) bool {
			return item.CanonicalName() == other.CanonicalName()
		})
	})
	r.logger.Debugf(
		"Chunks to be added publication: %+v",
		supporting.Map(initialChunkTables, func(chunk systemcatalog.SystemEntity) string {
			return chunk.TableName()
		}),
	)
	return initialChunkTables, nil
}

func getKnownChunks(encodedState func(name string) ([]byte, bool),
	getAllChunks func() []systemcatalog.SystemEntity) ([]systemcatalog.SystemEntity, error) {

	allChunks := getAllChunks()
	if state, present := encodedState(esPreviouslyKnownChunks); present {
		candidates, err := decodeKnownChunks(state)
		if err != nil {
			return nil, err
		}

		// Filter potentially deleted chunks
		return supporting.Filter(candidates, func(item systemcatalog.SystemEntity) bool {
			return supporting.IndexOfWithMatcher(allChunks, func(other systemcatalog.SystemEntity) bool {
				return item.CanonicalName() == other.CanonicalName()
			}) > -1
		}), nil
	}

	return allChunks, nil
}

func decodeKnownChunks(data []byte) ([]systemcatalog.SystemEntity, error) {
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

func encodeKnownChunks(chunks []systemcatalog.SystemEntity) ([]byte, error) {
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
