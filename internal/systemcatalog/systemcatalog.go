package systemcatalog

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal/configuring/sysconfig"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/replication/channels"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/filtering"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/snapshotting"
	"regexp"
)

var logger = logging.NewLogger("SystemCatalog")

var prefixExtractor = regexp.MustCompile("(distributed)?(compressed)?(_hyper_[0-9]+)_[0-9]+_chunk")

type SystemCatalog struct {
	databaseName          string
	hypertables           map[int32]*model.Hypertable
	chunks                map[int32]*model.Chunk
	hypertableNameIndex   map[string]int32
	chunkNameIndex        map[string]int32
	chunkTablePrefixIndex map[string]int32
	chunk2Hypertable      map[int32]int32
	hypertable2chunks     map[int32][]int32
	hypertable2compressed map[int32]int32
	compressed2hypertable map[int32]int32
	schemaRegistry        *schema.Registry
	topicNameGenerator    *topic.NameGenerator
	dispatcher            *eventhandler.Dispatcher
	sideChannel           channels.SideChannel
	replicationFilter     *filtering.TableFilter
	snapshotter           *snapshotting.Snapshotter
}

func NewSystemCatalog(config *sysconfig.SystemConfig, topicNameGenerator *topic.NameGenerator,
	dispatcher *eventhandler.Dispatcher, sideChannel channels.SideChannel, schemaRegistry *schema.Registry,
	snapshotter *snapshotting.Snapshotter) (*SystemCatalog, error) {

	// Create the Replication Filter, selecting enabled and blocking disabled hypertables for replication
	filterDefinition := config.Config.TimescaleDB.Hypertables
	replicationFilter, err := filtering.NewTableFilter(filterDefinition.Excludes, filterDefinition.Includes, false)
	if err != nil {
		return nil, err
	}

	return initializeSystemCatalog(&SystemCatalog{
		hypertables:           make(map[int32]*model.Hypertable, 0),
		chunks:                make(map[int32]*model.Chunk, 0),
		hypertableNameIndex:   make(map[string]int32),
		chunkNameIndex:        make(map[string]int32),
		chunkTablePrefixIndex: make(map[string]int32),
		chunk2Hypertable:      make(map[int32]int32),
		hypertable2chunks:     make(map[int32][]int32),
		hypertable2compressed: make(map[int32]int32),
		compressed2hypertable: make(map[int32]int32),

		databaseName:       config.PgxConfig.Database,
		schemaRegistry:     schemaRegistry,
		topicNameGenerator: topicNameGenerator,
		dispatcher:         dispatcher,
		sideChannel:        sideChannel,
		replicationFilter:  replicationFilter,
		snapshotter:        snapshotter,
	})
}

func (sc *SystemCatalog) FindHypertableById(hypertableId int32) *model.Hypertable {
	return sc.hypertables[hypertableId]
}

func (sc *SystemCatalog) FindHypertableByAssociatedPrefix(schemaName, prefix string) *model.Hypertable {
	if id, ok := sc.chunkTablePrefixIndex[model.MakeRelationKey(schemaName, prefix)]; ok {
		return sc.hypertables[id]
	}
	return nil
}

func (sc *SystemCatalog) FindHypertableByChunkName(schemaName, tableName string) *model.Hypertable {
	token := prefixExtractor.FindStringSubmatch(tableName)
	prefix := token[3]
	if id, ok := sc.chunkTablePrefixIndex[model.MakeRelationKey(schemaName, prefix)]; ok {
		return sc.hypertables[id]
	}
	return nil
}

func (sc *SystemCatalog) FindHypertableByName(schema, name string) *model.Hypertable {
	if id, ok := sc.hypertableNameIndex[model.MakeRelationKey(schema, name)]; ok {
		return sc.hypertables[id]
	}
	return nil
}

func (sc *SystemCatalog) FindHypertableByChunkId(chunkId int32) *model.Hypertable {
	if hypertableId, ok := sc.chunk2Hypertable[chunkId]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return nil
}

func (sc *SystemCatalog) FindHypertableByCompressedHypertableId(compressedHypertableId int32) *model.Hypertable {
	if hypertableId, ok := sc.compressed2hypertable[compressedHypertableId]; ok {
		return sc.FindHypertableById(hypertableId)
	}
	return nil
}

func (sc *SystemCatalog) FindCompressedHypertableByHypertableId(hypertableId int32) *model.Hypertable {
	if compressedHypertableId, ok := sc.hypertable2compressed[hypertableId]; ok {
		return sc.FindHypertableById(compressedHypertableId)
	}
	return nil
}

func (sc *SystemCatalog) FindChunkById(id int32) *model.Chunk {
	return sc.chunks[id]
}

func (sc *SystemCatalog) FindChunkByName(schemaName, tableName string) *model.Chunk {
	if id, ok := sc.chunkNameIndex[model.MakeRelationKey(schemaName, tableName)]; ok {
		return sc.chunks[id]
	}
	return nil
}

func (sc *SystemCatalog) ResolveOriginHypertable(chunk *model.Chunk) *model.Hypertable {
	return sc.ResolveUncompressedHypertable(chunk.HypertableId())
}

func (sc *SystemCatalog) ResolveUncompressedHypertable(hypertableId int32) *model.Hypertable {
	hypertable := sc.FindHypertableById(hypertableId)
	if hypertable == nil {
		return nil
	}

	if !hypertable.IsCompressedTable() {
		return hypertable
	}

	return sc.FindHypertableByCompressedHypertableId(hypertableId)
}

func (sc *SystemCatalog) IsHypertableSelectedForReplication(hypertableId int32) bool {
	hypertable := sc.ResolveUncompressedHypertable(hypertableId)
	if hypertable == nil {
		return false
	}

	return sc.replicationFilter.Enabled(hypertable)
}

func (sc *SystemCatalog) RegisterHypertable(hypertable *model.Hypertable) error {
	sc.hypertables[hypertable.Id()] = hypertable
	sc.chunkTablePrefixIndex[hypertable.CanonicalChunkTablePrefix()] = hypertable.Id()
	sc.hypertableNameIndex[hypertable.CanonicalName()] = hypertable.Id()
	sc.hypertable2chunks[hypertable.Id()] = make([]int32, 0)
	if compressedHypertableId, ok := hypertable.CompressedHypertableId(); ok {
		sc.hypertable2compressed[hypertable.Id()] = compressedHypertableId
		sc.compressed2hypertable[compressedHypertableId] = hypertable.Id()
	}
	logger.Printf("ADDED CATALOG ENTRY: HYPERTABLE %d => %+v", hypertable.Id(), hypertable)
	return nil
}

func (sc *SystemCatalog) UnregisterHypertable(hypertable *model.Hypertable) error {
	delete(sc.hypertables, hypertable.Id())
	delete(sc.chunkTablePrefixIndex, hypertable.CanonicalChunkTablePrefix())
	delete(sc.hypertableNameIndex, hypertable.CanonicalName())
	delete(sc.hypertable2compressed, hypertable.Id())
	delete(sc.compressed2hypertable, hypertable.Id())
	delete(sc.hypertable2chunks, hypertable.Id())
	logger.Printf("REMOVED CATALOG ENTRY: HYPERTABLE %d", hypertable.Id())
	return nil
}

func (sc *SystemCatalog) RegisterChunk(chunk *model.Chunk) error {
	sc.chunks[chunk.Id()] = chunk
	if hypertable := sc.FindHypertableById(chunk.HypertableId()); hypertable != nil {
		sc.chunkNameIndex[chunk.CanonicalName()] = chunk.Id()
		sc.chunk2Hypertable[chunk.Id()] = chunk.HypertableId()
		sc.hypertable2chunks[hypertable.Id()] = append(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
		logger.Printf("ADDED CATALOG ENTRY: CHUNK %d FOR HYPERTABLE %s => %+v",
			chunk.Id(), hypertable.CanonicalName(), *chunk)
	}
	return nil
}

func (sc *SystemCatalog) UnregisterChunk(chunk *model.Chunk) error {
	if hypertable := sc.FindHypertableByChunkId(chunk.Id()); hypertable != nil {
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

func (sc *SystemCatalog) NewEventHandler() eventhandler.SystemCatalogReplicationEventHandler {
	return &systemCatalogReplicationEventHandler{systemCatalog: sc}
}

func (sc *SystemCatalog) ApplySchemaUpdate(hypertable *model.Hypertable, columns []model.Column) bool {
	if difference := hypertable.ApplyTableSchema(columns); difference != nil {
		hypertableSchemaName := fmt.Sprintf("%s.Value", sc.topicNameGenerator.SchemaTopicName(hypertable))
		hypertableSchema := schema.HypertableSchema(hypertableSchemaName, hypertable.Columns())
		sc.schemaRegistry.RegisterSchema(hypertableSchemaName, hypertableSchema)
		logger.Printf("SCHEMA UPDATE: HYPERTABLE %d => %+v", hypertable.Id(), difference)
		return len(difference) > 0
	}
	return false
}

func (sc *SystemCatalog) GetAllChunks() []string {
	chunkTables := make([]string, 0)
	for _, chunk := range sc.chunks {
		if sc.IsHypertableSelectedForReplication(chunk.HypertableId()) {
			chunkTables = append(chunkTables, chunk.CanonicalName())
		}
	}
	return chunkTables
}

func (sc *SystemCatalog) snapshotChunk(chunk *model.Chunk) error {
	hypertable := sc.FindHypertableById(chunk.HypertableId())
	return sc.snapshotter.EnqueueSnapshot(snapshotting.SnapshotTask{
		Hypertable: hypertable,
		Chunk:      chunk,
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
	if err := sc.sideChannel.ReadHypertables(func(hypertable *model.Hypertable) error {
		if !sc.replicationFilter.Enabled(hypertable) {
			return nil
		}
		return sc.RegisterHypertable(hypertable)
	}); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	if err := sc.sideChannel.ReadChunks(sc.RegisterChunk); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	hypertables := make([]*model.Hypertable, 0)
	for _, hypertable := range sc.hypertables {
		hypertables = append(hypertables, hypertable)
	}

	if err := sc.sideChannel.ReadHypertableSchema(sc.ApplySchemaUpdate, hypertables...); err != nil {
		return nil, errors.Wrap(err, 0)
	}

	logger.Println("Selected hypertables for replication:")
	//atLeastOneHypertableSelected := false
	for _, hypertable := range sc.hypertables {
		if !hypertable.IsCompressedTable() && sc.IsHypertableSelectedForReplication(hypertable.Id()) {
			if hypertable.IsContinuousAggregate() {
				logger.Printf("\t* %s (type: Continuous Aggregate => %s)\n",
					hypertable.CanonicalContinuousAggregateName(), hypertable.CanonicalName(),
				)
			} else {
				logger.Printf("\t* %s (type: Hypertable)\n", hypertable.CanonicalName())
			}
			//atLeastOneHypertableSelected = true
		}
	}

	/*if !atLeastOneHypertableSelected {
		logger.Println("No hypertable was selected, exiting.")
		os.Exit(11)
	}*/

	return sc, nil
}
