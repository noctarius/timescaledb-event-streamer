package systemcatalog

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/snapshotting"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/tablefiltering"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
)

type SystemCatalog struct {
	hypertables           map[int32]*systemcatalog.Hypertable
	chunks                map[int32]*systemcatalog.Chunk
	hypertableNameIndex   map[string]int32
	chunkNameIndex        map[string]int32
	chunk2Hypertable      map[int32]int32
	hypertable2chunks     map[int32][]int32
	hypertable2compressed map[int32]int32
	compressed2hypertable map[int32]int32
	replicationContext    *context.ReplicationContext
	replicationFilter     *tablefiltering.TableFilter
	snapshotter           *snapshotting.Snapshotter
	logger                *logging.Logger
}

func NewSystemCatalog(config *config.Config, replicationContext *context.ReplicationContext,
	snapshotter *snapshotting.Snapshotter) (*SystemCatalog, error) {

	// Create the Replication Filter, selecting enabled and blocking disabled hypertables for replication
	filterDefinition := config.TimescaleDB.Hypertables
	replicationFilter, err := tablefiltering.NewTableFilter(filterDefinition.Excludes, filterDefinition.Includes, false)
	if err != nil {
		return nil, err
	}

	logger, err := logging.NewLogger("SystemCatalog")
	if err != nil {
		return nil, err
	}

	return initializeSystemCatalog(&SystemCatalog{
		hypertables:           make(map[int32]*systemcatalog.Hypertable, 0),
		chunks:                make(map[int32]*systemcatalog.Chunk, 0),
		hypertableNameIndex:   make(map[string]int32),
		chunkNameIndex:        make(map[string]int32),
		chunk2Hypertable:      make(map[int32]int32),
		hypertable2chunks:     make(map[int32][]int32),
		hypertable2compressed: make(map[int32]int32),
		compressed2hypertable: make(map[int32]int32),

		replicationContext: replicationContext,
		replicationFilter:  replicationFilter,
		snapshotter:        snapshotter,
		logger:             logger,
	})
}

func (sc *SystemCatalog) FindHypertableById(hypertableId int32) (hypertable *systemcatalog.Hypertable, present bool) {
	hypertable, present = sc.hypertables[hypertableId]
	return
}

func (sc *SystemCatalog) FindHypertableByName(
	schema, name string) (hypertable *systemcatalog.Hypertable, present bool) {

	if hypertableId, ok := sc.hypertableNameIndex[systemcatalog.MakeRelationKey(schema, name)]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return
}

func (sc *SystemCatalog) FindHypertableByChunkId(chunkId int32) (hypertable *systemcatalog.Hypertable, present bool) {
	if hypertableId, ok := sc.chunk2Hypertable[chunkId]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return
}

func (sc *SystemCatalog) FindHypertableByCompressedHypertableId(
	compressedHypertableId int32) (hypertable *systemcatalog.Hypertable, present bool) {

	if hypertableId, ok := sc.compressed2hypertable[compressedHypertableId]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return
}

func (sc *SystemCatalog) FindCompressedHypertableByHypertableId(
	hypertableId int32) (hypertable *systemcatalog.Hypertable, present bool) {

	if compressedHypertableId, ok := sc.hypertable2compressed[hypertableId]; ok {
		return sc.FindHypertableById(compressedHypertableId)
	}
	return
}

func (sc *SystemCatalog) FindChunkById(id int32) (chunk *systemcatalog.Chunk, present bool) {
	chunk, present = sc.chunks[id]
	return
}

func (sc *SystemCatalog) FindChunkByName(schemaName, tableName string) (chunk *systemcatalog.Chunk, present bool) {
	if chunkId, ok := sc.chunkNameIndex[systemcatalog.MakeRelationKey(schemaName, tableName)]; ok {
		return sc.FindChunkById(chunkId)
	}
	return
}

func (sc *SystemCatalog) ResolveOriginHypertable(
	chunk *systemcatalog.Chunk) (hypertable *systemcatalog.Hypertable, present bool) {

	hypertable, _, present = sc.ResolveUncompressedHypertable(chunk.HypertableId())
	return
}

func (sc *SystemCatalog) ResolveUncompressedHypertable(
	hypertableId int32) (uncompressedHypertable, compressedHypertable *systemcatalog.Hypertable, present bool) {

	if uncompressedHypertable, present = sc.FindHypertableById(hypertableId); present {
		if !uncompressedHypertable.IsCompressedTable() {
			return
		}

		compressedHypertable = uncompressedHypertable
		if uncompressedHypertable, present = sc.FindHypertableByCompressedHypertableId(hypertableId); present {
			return uncompressedHypertable, compressedHypertable, true
		}
		return nil, compressedHypertable, false
	}
	return
}

func (sc *SystemCatalog) IsHypertableSelectedForReplication(hypertableId int32) bool {
	if uncompressedHypertable, _, present := sc.ResolveUncompressedHypertable(hypertableId); present {
		return sc.replicationFilter.Enabled(uncompressedHypertable)
	}
	return false
}

func (sc *SystemCatalog) RegisterHypertable(hypertable *systemcatalog.Hypertable) error {
	sc.hypertables[hypertable.Id()] = hypertable
	sc.hypertableNameIndex[hypertable.CanonicalName()] = hypertable.Id()
	sc.hypertable2chunks[hypertable.Id()] = make([]int32, 0)
	if compressedHypertableId, ok := hypertable.CompressedHypertableId(); ok {
		sc.hypertable2compressed[hypertable.Id()] = compressedHypertableId
		sc.compressed2hypertable[compressedHypertableId] = hypertable.Id()
	}
	return nil
}

func (sc *SystemCatalog) UnregisterHypertable(hypertable *systemcatalog.Hypertable) error {
	delete(sc.hypertables, hypertable.Id())
	delete(sc.hypertableNameIndex, hypertable.CanonicalName())
	delete(sc.hypertable2compressed, hypertable.Id())
	delete(sc.compressed2hypertable, hypertable.Id())
	delete(sc.hypertable2chunks, hypertable.Id())
	sc.logger.Verbosef("REMOVED CATALOG ENTRY: HYPERTABLE %d", hypertable.Id())
	return nil
}

func (sc *SystemCatalog) RegisterChunk(chunk *systemcatalog.Chunk) error {
	if hypertable, present := sc.FindHypertableById(chunk.HypertableId()); present {
		sc.chunks[chunk.Id()] = chunk
		sc.chunkNameIndex[chunk.CanonicalName()] = chunk.Id()
		sc.chunk2Hypertable[chunk.Id()] = chunk.HypertableId()
		sc.hypertable2chunks[hypertable.Id()] = append(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
	}
	return nil
}

func (sc *SystemCatalog) UnregisterChunk(chunk *systemcatalog.Chunk) error {
	if hypertable, present := sc.FindHypertableByChunkId(chunk.Id()); present {
		index := indexOf(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
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

func (sc *SystemCatalog) NewEventHandler() eventhandlers.SystemCatalogReplicationEventHandler {
	return &systemCatalogReplicationEventHandler{systemCatalog: sc}
}

func (sc *SystemCatalog) ApplySchemaUpdate(hypertable *systemcatalog.Hypertable, columns []systemcatalog.Column) bool {
	if difference := hypertable.ApplyTableSchema(columns); difference != nil {
		hypertableSchemaName := fmt.Sprintf("%s.Value", sc.replicationContext.SchemaTopicName(hypertable))
		hypertableSchema := schema.HypertableSchema(hypertableSchemaName, hypertable.Columns())
		sc.replicationContext.RegisterSchema(hypertableSchemaName, hypertableSchema)
		sc.logger.Verbosef("SCHEMA UPDATE: HYPERTABLE %d => %+v", hypertable.Id(), difference)
		return len(difference) > 0
	}
	return false
}

func (sc *SystemCatalog) GetAllChunks() []systemcatalog.SystemEntity {
	chunkTables := make([]systemcatalog.SystemEntity, 0)
	for _, chunk := range sc.chunks {
		if sc.IsHypertableSelectedForReplication(chunk.HypertableId()) {
			chunkTables = append(chunkTables, chunk)
		}
	}
	return chunkTables
}

/* TODO: Commented out for now
func (sc *SystemCatalog) snapshotChunk(chunk *systemcatalog.Chunk) error {
	return sc.snapshotChunkWithXld(nil, chunk)
}*/

func (sc *SystemCatalog) snapshotChunkWithXld(xld *pgtypes.XLogData, chunk *systemcatalog.Chunk) error {
	if hypertable, present := sc.FindHypertableById(chunk.HypertableId()); present {
		return sc.snapshotter.EnqueueSnapshot(snapshotting.SnapshotTask{
			Hypertable: hypertable,
			Chunk:      chunk,
			Xld:        xld,
		})
	}
	return nil
}

func (sc *SystemCatalog) snapshotHypertable(snapshotName string, hypertable *systemcatalog.Hypertable) error {
	return sc.snapshotter.EnqueueSnapshot(snapshotting.SnapshotTask{
		Hypertable:   hypertable,
		SnapshotName: &snapshotName,
	})
}

func indexOf[T comparable](slice []T, item T) int {
	for i, x := range slice {
		if x == item {
			return i
		}
	}
	return -1
}

func initializeSystemCatalog(sc *SystemCatalog) (*SystemCatalog, error) {
	if err := sc.replicationContext.LoadHypertables(func(hypertable *systemcatalog.Hypertable) error {
		// Check if we want to replicate that hypertable
		if !sc.replicationFilter.Enabled(hypertable) {
			return nil
		}

		// Run basic access check based on user permissions
		access, err := sc.replicationContext.HasTablePrivilege(hypertable, context.Select)
		if err != nil {
			return err
		}
		if !access {
			return errors.Errorf("Hypertable %s not accessible", hypertable.CanonicalName())
		}

		if err := sc.RegisterHypertable(hypertable); err != nil {
			return errors.Errorf("registering hypertable failed: %v (error: %+v)", hypertable, err)
		}
		sc.logger.Verbosef("ADDED CATALOG ENTRY: HYPERTABLE %d => %+v", hypertable.Id(), hypertable)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	if err := sc.replicationContext.LoadChunks(func(chunk *systemcatalog.Chunk) error {
		if err := sc.RegisterChunk(chunk); err != nil {
			return errors.Errorf("registering chunk failed: %v (error: %+v)", chunk, err)
		}
		if h, present := sc.FindHypertableById(chunk.HypertableId()); present {
			sc.logger.Verbosef(
				"ADDED CATALOG ENTRY: CHUNK %d FOR HYPERTABLE %s => %+v",
				chunk.Id(), h.CanonicalName(), *chunk,
			)
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	hypertables := make([]*systemcatalog.Hypertable, 0)
	for _, hypertable := range sc.hypertables {
		hypertables = append(hypertables, hypertable)
	}

	if err := sc.replicationContext.ReadHypertableSchema(sc.ApplySchemaUpdate, hypertables...); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	sc.logger.Println("Selected hypertables for replication:")
	for _, hypertable := range sc.hypertables {
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
	sc.replicationContext.RegisterReplicationEventHandler(&snapshottingEventHandler{systemCatalog: sc})

	return sc, nil
}

type snapshottingEventHandler struct {
	systemCatalog      *SystemCatalog
	handledHypertables []string
}

func (s *snapshottingEventHandler) OnRelationEvent(_ pgtypes.XLogData, _ *pgtypes.RelationMessage) error {
	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotStartedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return nil
}

func (s *snapshottingEventHandler) OnChunkSnapshotFinishedEvent(
	_ *systemcatalog.Hypertable, _ *systemcatalog.Chunk, _ pgtypes.LSN) error {

	return nil
}

func (s *snapshottingEventHandler) OnHypertableSnapshotStartedEvent(
	snapshotName string, hypertable *systemcatalog.Hypertable) error {

	snapshotContext, err := s.systemCatalog.replicationContext.GetSnapshotContext()
	if err != nil {
		return err
	}

	if snapshotContext != nil {
		watermark, present := snapshotContext.GetWatermark(hypertable)
		if !present {
			s.systemCatalog.logger.Infof("Start snapshotting of hypertable '%s'", hypertable.CanonicalName())
		} else if watermark.Complete() {
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
	snapshotName string, hypertable *systemcatalog.Hypertable) error {

	if err := s.systemCatalog.replicationContext.SnapshotContextTransaction(
		snapshotName, false,
		func(snapshotContext *watermark.SnapshotContext) error {
			s.handledHypertables = append(s.handledHypertables, hypertable.CanonicalName())

			watermark, _ := snapshotContext.GetOrCreateWatermark(hypertable)
			watermark.MarkComplete()

			return nil
		},
	); err != nil {
		return errors.WrapPrefix(err, "illegal snapshot context state after snapshotting hypertable", 0)
	}

	s.systemCatalog.logger.Infof("Finished snapshotting for hypertable '%s'", hypertable.CanonicalName())
	return s.scheduleNextSnapshotHypertableOrFinish(snapshotName)
}

func (s *snapshottingEventHandler) OnSnapshottingStartedEvent(snapshotName string) error {
	s.systemCatalog.logger.Infoln("Start snapshotting hypertables")

	// No hypertables? Just start the replicator
	if len(s.systemCatalog.hypertables) == 0 {
		return s.systemCatalog.replicationContext.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnSnapshottingFinishedEvent()
			})
		})
	}

	// We are just starting out, get the first hypertable to start snapshotting
	var hypertable *systemcatalog.Hypertable
	for _, ht := range s.systemCatalog.hypertables {
		hypertable = ht
		break
	}

	return s.systemCatalog.replicationContext.EnqueueTask(func(notificator context.Notificator) {
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
	for _, ht := range s.systemCatalog.hypertables {
		alreadyHandled := false
		for _, h := range s.handledHypertables {
			if ht.CanonicalName() == h {
				alreadyHandled = true
				break
			}
		}

		if alreadyHandled {
			continue
		}

		return ht
	}
	return nil
}

func (s *snapshottingEventHandler) scheduleNextSnapshotHypertableOrFinish(snapshotName string) error {
	if nextHypertable := s.nextSnapshotHypertable(); nextHypertable != nil {
		return s.systemCatalog.replicationContext.EnqueueTask(func(notificator context.Notificator) {
			notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
				return handler.OnHypertableSnapshotStartedEvent(snapshotName, nextHypertable)
			})
		})
	}

	// Seems like all hypertables are handles successfully, let's finish up and start the replicator
	return s.systemCatalog.replicationContext.EnqueueTask(func(notificator context.Notificator) {
		notificator.NotifySnapshottingEventHandler(func(handler eventhandlers.SnapshottingEventHandler) error {
			return handler.OnSnapshottingFinishedEvent()
		})
	})
}
