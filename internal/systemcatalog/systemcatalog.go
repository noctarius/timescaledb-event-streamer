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

package systemcatalog

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/tablefiltering"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/publication"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/task"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
	"github.com/samber/lo"
	"sync"
)

type systemCatalog struct {
	hypertables           map[int32]*systemcatalog.Hypertable
	chunks                map[int32]*systemcatalog.Chunk
	hypertableNameIndex   map[string]int32
	chunkNameIndex        map[string]int32
	chunk2Hypertable      map[int32]int32
	hypertable2chunks     map[int32][]int32
	hypertable2compressed map[int32]int32
	compressed2hypertable map[int32]int32

	username string

	publicationManager  publication.PublicationManager
	sideChannel         sidechannel.SideChannel
	stateStorageManager statestorage.Manager
	typeManager         pgtypes.TypeManager
	taskManager         task.TaskManager
	replicationFilter   *tablefiltering.TableFilter
	snapshotter         *snapshotting.Snapshotter
	logger              *logging.Logger
	rwLock              sync.RWMutex
}

func NewSystemCatalog(
	config *config.Config, userConfig *pgx.ConnConfig, sideChannel sidechannel.SideChannel,
	typeManager pgtypes.TypeManager, snapshotter *snapshotting.Snapshotter, taskManager task.TaskManager,
	publicationManager publication.PublicationManager, stateStorageManager statestorage.Manager,
) (systemcatalog.SystemCatalog, error) {

	// Create the Replication Filter, selecting enabled and blocking disabled hypertables for replication
	filterDefinition := config.TimescaleDB.Hypertables
	replicationFilter, err := tablefiltering.NewTableFilter(filterDefinition.Excludes, filterDefinition.Includes, false)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	logger, err := logging.NewLogger("SystemCatalog")
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	return &systemCatalog{
		hypertables:           make(map[int32]*systemcatalog.Hypertable),
		chunks:                make(map[int32]*systemcatalog.Chunk),
		hypertableNameIndex:   make(map[string]int32),
		chunkNameIndex:        make(map[string]int32),
		chunk2Hypertable:      make(map[int32]int32),
		hypertable2chunks:     make(map[int32][]int32),
		hypertable2compressed: make(map[int32]int32),
		compressed2hypertable: make(map[int32]int32),

		username: userConfig.User,

		stateStorageManager: stateStorageManager,
		replicationFilter:   replicationFilter,
		publicationManager:  publicationManager,
		sideChannel:         sideChannel,
		snapshotter:         snapshotter,
		typeManager:         typeManager,
		taskManager:         taskManager,
		logger:              logger,
	}, nil
}

func (sc *systemCatalog) PostConstruct() error {
	if _, err := initializeSystemCatalog(sc); err != nil {
		return err
	}
	sc.taskManager.RegisterReplicationEventHandler(sc.newEventHandler())
	return nil
}

func (sc *systemCatalog) FindHypertableById(
	hypertableId int32,
) (hypertable *systemcatalog.Hypertable, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	hypertable, present = sc.hypertables[hypertableId]
	return
}

func (sc *systemCatalog) FindHypertableByName(
	schema, name string,
) (hypertable *systemcatalog.Hypertable, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	if hypertableId, ok := sc.hypertableNameIndex[systemcatalog.MakeRelationKey(schema, name)]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return
}

func (sc *systemCatalog) FindHypertableByChunkId(
	chunkId int32,
) (hypertable *systemcatalog.Hypertable, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	if hypertableId, ok := sc.chunk2Hypertable[chunkId]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return
}

func (sc *systemCatalog) FindHypertableByCompressedHypertableId(
	compressedHypertableId int32,
) (hypertable *systemcatalog.Hypertable, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	if hypertableId, ok := sc.compressed2hypertable[compressedHypertableId]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return
}

func (sc *systemCatalog) FindCompressedHypertableByHypertableId(
	hypertableId int32,
) (hypertable *systemcatalog.Hypertable, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	if compressedHypertableId, ok := sc.hypertable2compressed[hypertableId]; ok {
		return sc.FindHypertableById(compressedHypertableId)
	}
	return
}

func (sc *systemCatalog) FindChunkById(
	id int32,
) (chunk *systemcatalog.Chunk, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	chunk, present = sc.chunks[id]
	return
}

func (sc *systemCatalog) FindChunkByName(
	schemaName, tableName string,
) (chunk *systemcatalog.Chunk, present bool) {

	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	if chunkId, ok := sc.chunkNameIndex[systemcatalog.MakeRelationKey(schemaName, tableName)]; ok {
		return sc.FindChunkById(chunkId)
	}
	return
}

func (sc *systemCatalog) ResolveOriginHypertable(
	chunk *systemcatalog.Chunk,
) (hypertable *systemcatalog.Hypertable, present bool) {

	hypertable, _, present = sc.ResolveUncompressedHypertable(chunk.HypertableId())
	return
}

func (sc *systemCatalog) ResolveUncompressedHypertable(
	hypertableId int32,
) (uncompressedHypertable, compressedHypertable *systemcatalog.Hypertable, present bool) {

	if uncompressedHypertable, present = sc.FindHypertableById(hypertableId); present {
		if !uncompressedHypertable.IsCompressedTable() {
			return uncompressedHypertable, nil, true
		}

		compressedHypertable = uncompressedHypertable
		uncompressedHypertable, present = sc.FindHypertableByCompressedHypertableId(hypertableId)
		return uncompressedHypertable, compressedHypertable, present
	}
	return nil, nil, false
}

func (sc *systemCatalog) IsHypertableSelectedForReplication(
	hypertableId int32,
) bool {

	if uncompressedHypertable, _, present := sc.ResolveUncompressedHypertable(hypertableId); present {
		return sc.replicationFilter.Enabled(uncompressedHypertable)
	}
	return false
}

func (sc *systemCatalog) RegisterHypertable(
	hypertable *systemcatalog.Hypertable,
) error {

	sc.rwLock.Lock()
	defer sc.rwLock.Unlock()
	sc.hypertables[hypertable.Id()] = hypertable
	sc.hypertableNameIndex[hypertable.CanonicalName()] = hypertable.Id()
	sc.hypertable2chunks[hypertable.Id()] = make([]int32, 0)
	if compressedHypertableId, ok := hypertable.CompressedHypertableId(); ok {
		sc.hypertable2compressed[hypertable.Id()] = compressedHypertableId
		sc.compressed2hypertable[compressedHypertableId] = hypertable.Id()
	}
	return nil
}

func (sc *systemCatalog) UnregisterHypertable(
	hypertable *systemcatalog.Hypertable,
) error {

	sc.rwLock.Lock()
	defer sc.rwLock.Unlock()
	delete(sc.hypertables, hypertable.Id())
	delete(sc.hypertableNameIndex, hypertable.CanonicalName())
	delete(sc.hypertable2compressed, hypertable.Id())
	delete(sc.compressed2hypertable, hypertable.Id())
	delete(sc.hypertable2chunks, hypertable.Id())
	sc.logger.Verbosef("Entry Dropped: Hypertable %d", hypertable.Id())
	return nil
}

func (sc *systemCatalog) RegisterChunk(
	chunk *systemcatalog.Chunk,
) error {

	if hypertable, present := sc.FindHypertableById(chunk.HypertableId()); present {
		sc.rwLock.Lock()
		defer sc.rwLock.Unlock()
		sc.chunks[chunk.Id()] = chunk
		sc.chunkNameIndex[chunk.CanonicalName()] = chunk.Id()
		sc.chunk2Hypertable[chunk.Id()] = chunk.HypertableId()
		sc.hypertable2chunks[hypertable.Id()] = append(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
	}
	return nil
}

func (sc *systemCatalog) UnregisterChunk(
	chunk *systemcatalog.Chunk,
) error {

	hypertable, present := sc.FindHypertableByChunkId(chunk.Id())

	sc.rwLock.Lock()
	defer sc.rwLock.Unlock()
	if present {
		index := lo.IndexOf(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
		// Erase element (zero value) to prevent memory leak
		if index >= 0 {
			sc.hypertable2chunks[hypertable.Id()][index] = 0
			sc.hypertable2chunks[hypertable.Id()] = append(
				sc.hypertable2chunks[hypertable.Id()][:index],
				sc.hypertable2chunks[hypertable.Id()][index+1:]...,
			)
		}
	}
	delete(sc.chunkNameIndex, chunk.CanonicalName())
	delete(sc.chunk2Hypertable, chunk.Id())
	delete(sc.chunks, chunk.Id())
	return nil
}

func (sc *systemCatalog) ApplySchemaUpdate(
	hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column,
) bool {

	if difference := hypertable.ApplyTableSchema(columns); difference != nil {
		sc.logger.Verbosef("Schema Update: Hypertable %d => %+v", hypertable.Id(), difference)
		return len(difference) > 0
	}
	return false
}

func (sc *systemCatalog) GetAllChunks() []systemcatalog.SystemEntity {
	chunkTables := make([]systemcatalog.SystemEntity, 0)
	for _, chunk := range sc.chunks {
		if sc.IsHypertableSelectedForReplication(chunk.HypertableId()) {
			chunkTables = append(chunkTables, chunk)
		}
	}
	return chunkTables
}

func (sc *systemCatalog) snapshotChunkWithXld(
	xld *pgtypes.XLogData, chunk *systemcatalog.Chunk,
) error {

	if hypertable, present := sc.FindHypertableById(chunk.HypertableId()); present {
		return sc.snapshotter.EnqueueSnapshot(snapshotting.SnapshotTask{
			Hypertable: hypertable,
			Chunk:      chunk,
			Xld:        xld,
		})
	}
	return nil
}

func (sc *systemCatalog) snapshotHypertable(
	snapshotName string, hypertable *systemcatalog.Hypertable,
) error {

	return sc.snapshotter.EnqueueSnapshot(snapshotting.SnapshotTask{
		Hypertable:   hypertable,
		SnapshotName: &snapshotName,
	})
}

func (sc *systemCatalog) newEventHandler() eventhandlers.SystemCatalogReplicationEventHandler {
	return &systemCatalogReplicationEventHandler{systemCatalog: sc}
}

func initializeSystemCatalog(
	sc *systemCatalog,
) (*systemCatalog, error) {

	if err := sc.sideChannel.ReadHypertables(func(hypertable *systemcatalog.Hypertable) error {
		// Check if we want to replicate that hypertable
		if !sc.replicationFilter.Enabled(hypertable) {
			return nil
		}

		// Run basic access check based on user permissions
		access, err := sc.sideChannel.HasTablePrivilege(sc.username, hypertable, sidechannel.Select)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		if !access {
			return errors.Errorf("Hypertable %s not accessible", hypertable.CanonicalName())
		}

		if err := sc.RegisterHypertable(hypertable); err != nil {
			return errors.Errorf("registering hypertable failed: %s (error: %+v)", hypertable, err)
		}
		sc.logger.Verbosef("Entry Added: Hypertable %d => %s", hypertable.Id(), hypertable)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	if err := sc.sideChannel.ReadChunks(func(chunk *systemcatalog.Chunk) error {
		if err := sc.RegisterChunk(chunk); err != nil {
			return errors.Errorf("registering chunk failed: %s (error: %+v)", chunk, err)
		}
		if h, present := sc.FindHypertableById(chunk.HypertableId()); present {
			sc.logger.Verbosef(
				"Entry Added: Chunk %d for Hypertable %s => %s",
				chunk.Id(), h.CanonicalName(), chunk,
			)
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// No explicit logging, will not happen concurrently
	hypertables := make([]*systemcatalog.Hypertable, 0)
	for _, hypertable := range sc.hypertables {
		hypertables = append(hypertables, hypertable)
	}

	if err := sc.sideChannel.ReadHypertableSchema(
		sc.ApplySchemaUpdate, sc.typeManager.ResolveDataType, hypertables...,
	); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	sc.logger.Println("Selected hypertables for replication:")
	for _, hypertable := range hypertables {
		if !hypertable.IsCompressedTable() && sc.IsHypertableSelectedForReplication(hypertable.Id()) {
			if hypertable.IsContinuousAggregate() {
				sc.logger.Infof("  * %s (type: Continuous Aggregate => %s)",
					hypertable.CanonicalContinuousAggregateName(), hypertable.CanonicalName(),
				)
			} else {
				sc.logger.Infof("  * %s (type: Hypertable)", hypertable.CanonicalName())
			}
		}
	}

	// Register the snapshot event handler
	sc.taskManager.RegisterReplicationEventHandler(newSnapshottingEventHandler(sc))

	return sc, nil
}

type snapshottingEventHandler struct {
	systemCatalog      *systemCatalog
	taskManager        task.TaskManager
	handledHypertables map[string]bool
}

func newSnapshottingEventHandler(
	systemCatalog *systemCatalog,
) *snapshottingEventHandler {

	return &snapshottingEventHandler{
		systemCatalog:      systemCatalog,
		taskManager:        systemCatalog.taskManager,
		handledHypertables: make(map[string]bool),
	}
}

func (s *snapshottingEventHandler) OnRelationEvent(
	_ pgtypes.XLogData, _ *pgtypes.RelationMessage,
) error {
	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotStartedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotFinishedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk, _ pgtypes.LSN,
) error {

	return nil
}

func (s *snapshottingEventHandler) OnHypertableSnapshotStartedEvent(
	snapshotName string, hypertable *systemcatalog.Hypertable,
) error {

	stateManager := s.systemCatalog.stateStorageManager
	snapshotContext, err := stateManager.SnapshotContext()
	if err != nil {
		return err
	}

	if snapshotContext != nil {
		hypertableWatermark, present := snapshotContext.GetWatermark(hypertable)
		if !present {
			s.systemCatalog.logger.Infof("Start snapshotting of hypertable '%s'", hypertable.CanonicalName())
		} else if hypertableWatermark.Complete() {
			s.systemCatalog.logger.Infof(
				"Snapshotting for hypertable '%s' already completed, skipping", hypertable.CanonicalName(),
			)
			return s.scheduleNextSnapshotHypertableOrFinish(snapshotName)
		} else {
			s.systemCatalog.logger.Infof("Resuming snapshotting of hypertable '%s'", hypertable.CanonicalName())
		}
	} else {
		s.systemCatalog.logger.Infof("Start snapshotting of hypertable '%s'", hypertable.CanonicalName())
	}

	if err := s.systemCatalog.snapshotHypertable(snapshotName, hypertable); err != nil {
		return err
	}

	return nil
}

func (s *snapshottingEventHandler) OnHypertableSnapshotFinishedEvent(
	snapshotName string, hypertable *systemcatalog.Hypertable,
) error {

	stateManager := s.systemCatalog.stateStorageManager
	if err := stateManager.SnapshotContextTransaction(
		snapshotName, false,
		func(snapshotContext *watermark.SnapshotContext) error {
			s.handledHypertables[hypertable.CanonicalName()] = true

			hypertableWatermark, _ := snapshotContext.GetOrCreateWatermark(hypertable)
			hypertableWatermark.MarkComplete()

			return nil
		},
	); err != nil {
		return errors.WrapPrefix(err, "illegal snapshot context state after snapshotting hypertable", 0)
	}

	s.systemCatalog.logger.Infof("Finished snapshotting for hypertable '%s'", hypertable.CanonicalName())
	return s.scheduleNextSnapshotHypertableOrFinish(snapshotName)
}

func (s *snapshottingEventHandler) OnSnapshottingStartedEvent(
	snapshotName string,
) error {

	s.systemCatalog.logger.Infoln("Start snapshotting hypertables")

	// No hypertables? Just start the replicator
	if len(s.systemCatalog.hypertables) == 0 {
		return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnSnapshottingFinishedEvent()
			})
		})
	}

	// We are just starting out, get the first hypertable to start snapshotting
	// No explicit logging, will not happen concurrently
	var hypertable *systemcatalog.Hypertable
	for _, ht := range s.systemCatalog.hypertables {
		hypertable = ht
		break
	}

	return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
			return handler.OnHypertableSnapshotStartedEvent(snapshotName, hypertable)
		})
	})
}

func (s *snapshottingEventHandler) OnSnapshottingFinishedEvent() error {
	s.systemCatalog.logger.Infoln("Finished snapshotting hypertables")
	return nil
}

func (s *snapshottingEventHandler) nextSnapshotHypertable() *systemcatalog.Hypertable {
	s.systemCatalog.rwLock.RLock()
	defer s.systemCatalog.rwLock.RUnlock()
	for _, ht := range s.systemCatalog.hypertables {
		if alreadyHandled, present := s.handledHypertables[ht.CanonicalName()]; !present || !alreadyHandled {
			return ht
		}
	}
	return nil
}

func (s *snapshottingEventHandler) scheduleNextSnapshotHypertableOrFinish(
	snapshotName string,
) error {

	if nextHypertable := s.nextSnapshotHypertable(); nextHypertable != nil {
		return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnHypertableSnapshotStartedEvent(snapshotName, nextHypertable)
			})
		})
	}

	// Seems like all hypertables are handles successfully, let's finish up and start the replicator
	return s.taskManager.EnqueueTask(func(notificator task.Notificator) {
		notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
			return handler.OnSnapshottingFinishedEvent()
		})
	})
}
