package systemcatalog

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/logging"
	"github.com/noctarius/event-stream-prototype/internal/replication/channel"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"os"
	"regexp"
)

var logger = logging.NewLogger("SystemCatalog")

const initialHypertableQuery = `
SELECT h1.id, h1.schema_name, h1.table_name, h1.associated_schema_name, h1.associated_table_prefix, 
	 h1.compression_state, h1.compressed_hypertable_id, coalesce(h2.is_distributed, false)
FROM _timescaledb_catalog.hypertable h1
LEFT JOIN timescaledb_information.hypertables h2 
	 ON h2.hypertable_schema = h1.schema_name 
	AND h2.hypertable_name = h1.table_name`

const initialChunkQuery = `
SELECT c1.id, c1.hypertable_id, c1.schema_name, c1.table_name, c1.compressed_chunk_id, c1.dropped, c1.status
FROM _timescaledb_catalog.chunk c1
LEFT JOIN timescaledb_information.chunks c2
       ON c2.chunk_schema = c1.schema_name
      AND c2.chunk_name = c1.table_name
LEFT JOIN _timescaledb_catalog.chunk c3 ON c3.compressed_chunk_id = c1.id
LEFT JOIN timescaledb_information.chunks c4
       ON c4.chunk_schema = c3.schema_name
      AND c4.chunk_name = c3.table_name
ORDER BY c1.hypertable_id, coalesce(c2.range_start, c4.range_start)
`

const initialTableSchema = `
SELECT
   c.column_name,
   t.oid::int,
   CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END,
   CASE WHEN c.is_identity = 'YES' THEN true ELSE false END,
   c.column_default
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = c.udt_schema
LEFT JOIN pg_catalog.pg_type t ON t.typnamespace = n.oid AND t.typname = c.udt_name
WHERE c.table_schema = '%s'
  AND c.table_name = '%s'
ORDER BY c.ordinal_position
`

const addTableToPublication = "ALTER PUBLICATION %s ADD TABLE %s"

//const dropTableToPublication = "ALTER PUBLICATION %s DROP TABLE %s"

var prefixExtractor = regexp.MustCompile("(distributed)?(compressed)?(_hyper_[0-9]+)_[0-9]+_chunk")

type SystemCatalog struct {
	databaseName          string
	config                *configuration.Config
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
	queryAdapter          channel.QueryAdapter
	replicationFilter     *replicationFilter
	typeMap               *pgtype.Map
}

func NewSystemCatalog(databaseName string, config *configuration.Config, schemaRegistry *schema.Registry,
	topicNameGenerator *topic.NameGenerator, dispatcher *eventhandler.Dispatcher,
	queryAdapter channel.QueryAdapter) (*SystemCatalog, error) {

	replicationFilter, err := newReplicationFilter(config)
	if err != nil {
		return nil, err
	}

	catalog := &SystemCatalog{
		databaseName:          databaseName,
		config:                config,
		hypertables:           make(map[int32]*model.Hypertable, 0),
		chunks:                make(map[int32]*model.Chunk, 0),
		hypertableNameIndex:   make(map[string]int32),
		chunkNameIndex:        make(map[string]int32),
		chunkTablePrefixIndex: make(map[string]int32),
		chunk2Hypertable:      make(map[int32]int32),
		hypertable2chunks:     make(map[int32][]int32),
		hypertable2compressed: make(map[int32]int32),
		compressed2hypertable: make(map[int32]int32),
		schemaRegistry:        schemaRegistry,
		topicNameGenerator:    topicNameGenerator,
		dispatcher:            dispatcher,
		queryAdapter:          queryAdapter,
		replicationFilter:     replicationFilter,
		typeMap:               pgtype.NewMap(),
	}

	if err := queryAdapter.NewSession(func(session channel.QuerySession) error {
		if err := session.QueryFunc(context.Background(), func(row pgx.Row) error {
			var id int32
			var schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string
			var compressionState int16
			var compressedHypertableId *int32
			var distributed bool

			if err := row.Scan(&id, &schemaName, &hypertableName, &associatedSchemaName,
				&associatedTablePrefix, &compressionState, &compressedHypertableId, &distributed); err != nil {
				return errors.Wrap(err, 0)
			}

			hypertable := model.NewHypertable(id, databaseName, schemaName, hypertableName, associatedSchemaName,
				associatedTablePrefix, compressedHypertableId, compressionState, distributed)

			logger.Printf("ADDED CATALOG ENTRY: HYPERTABLE %d => %+v", hypertable.Id(), hypertable)
			return catalog.RegisterHypertable(hypertable)
		}, initialHypertableQuery); err != nil {
			return errors.Wrap(err, 0)
		}

		if err := session.QueryFunc(context.Background(), func(row pgx.Row) error {
			var id, hypertableId int32
			var schemaName, tableName string
			var compressedChunkId *int32
			var dropped bool
			var status int32

			if err := row.Scan(&id, &hypertableId, &schemaName, &tableName,
				&compressedChunkId, &dropped, &status); err != nil {
				return errors.Wrap(err, 0)
			}

			return catalog.RegisterChunk(
				model.NewChunk(id, hypertableId, schemaName, tableName, dropped, status, compressedChunkId),
			)
		}, initialChunkQuery); err != nil {
			return errors.Wrap(err, 0)
		}

		for _, hypertable := range catalog.hypertables {
			if hypertable.SchemaName() == "_timescaledb_internal" ||
				hypertable.SchemaName() == "_timescaledb_catalog" {

				continue
			}

			columns, err := catalog.readHypertableSchema(session, hypertable)
			if err != nil {
				return errors.Wrap(err, 0)
			}
			catalog.ApplySchemaUpdate(hypertable, columns)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	logger.Println("Selected hypertables for replication:")
	atLeastOneHypertableSelected := false
	for _, hypertable := range catalog.hypertables {
		if !hypertable.IsCompressedTable() && catalog.IsHypertableSelectedForReplication(hypertable.Id()) {
			logger.Printf("\t* %s\n", hypertable.CanonicalName())
			atLeastOneHypertableSelected = true
		}
	}

	if !atLeastOneHypertableSelected {
		logger.Println("No hypertable was selected, exiting.")
		os.Exit(11)
	}

	return catalog, nil
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

	return sc.replicationFilter.enabled(hypertable)
}

/*func (sc *SystemCatalog) IterateChunks(hypertable *model.Hypertable, fn func(chunk *model.Chunk) error) error {
	for _, chunkId := range sc.hypertable2chunks[hypertable.Id()] {
		if err := fn(sc.FindChunkById(chunkId)); err != nil {
			return err
		}
	}
	return nil
}*/

func (sc *SystemCatalog) RegisterHypertable(hypertable *model.Hypertable) error {
	sc.hypertables[hypertable.Id()] = hypertable
	sc.chunkTablePrefixIndex[hypertable.CanonicalChunkTablePrefix()] = hypertable.Id()
	sc.hypertableNameIndex[hypertable.CanonicalName()] = hypertable.Id()
	sc.hypertable2chunks[hypertable.Id()] = make([]int32, 0)
	if compressedHypertableId, ok := hypertable.CompressedHypertableId(); ok {
		sc.hypertable2compressed[hypertable.Id()] = compressedHypertableId
		sc.compressed2hypertable[compressedHypertableId] = hypertable.Id()
	}
	return nil
}

func (sc *SystemCatalog) UnregisterHypertable(hypertable *model.Hypertable) error {
	delete(sc.hypertables, hypertable.Id())
	delete(sc.chunkTablePrefixIndex, hypertable.CanonicalChunkTablePrefix())
	delete(sc.hypertableNameIndex, hypertable.CanonicalName())
	delete(sc.hypertable2compressed, hypertable.Id())
	delete(sc.compressed2hypertable, hypertable.Id())
	delete(sc.hypertable2chunks, hypertable.Id())
	return nil
}

func (sc *SystemCatalog) RegisterChunk(chunk *model.Chunk) error {
	sc.chunks[chunk.Id()] = chunk
	if hypertable := sc.FindHypertableById(chunk.HypertableId()); hypertable != nil {
		sc.chunkNameIndex[chunk.CanonicalName()] = chunk.Id()
		sc.chunk2Hypertable[chunk.Id()] = chunk.HypertableId()
		sc.hypertable2chunks[hypertable.Id()] = append(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
	}
	return nil
}

func (sc *SystemCatalog) UnregisterChunk(chunk *model.Chunk) error {
	if hypertable := sc.FindHypertableByChunkId(chunk.Id()); hypertable != nil {
		index := indexOf(sc.hypertable2chunks[hypertable.Id()], chunk.Id())
		// Erase element (zero value) to prevent memory leak
		sc.hypertable2chunks[hypertable.Id()][index] = 0
		sc.hypertable2chunks[hypertable.Id()] = append(
			sc.hypertable2chunks[hypertable.Id()][:index],
			sc.hypertable2chunks[hypertable.Id()][index+1:]...,
		)
	}
	delete(sc.chunkNameIndex, chunk.CanonicalName())
	delete(sc.chunk2Hypertable, chunk.Id())
	delete(sc.chunks, chunk.Id())
	return nil
}

func (sc *SystemCatalog) NewEventHandler() eventhandler.SystemCatalogReplicationEventHandler {
	return &systemCatalogReplicationEventHandler{systemCatalog: sc}
}

func (sc *SystemCatalog) ApplySchemaUpdate(hypertable *model.Hypertable, columns []model.Column) {
	if difference := hypertable.ApplyTableSchema(columns); difference != nil {
		hypertableSchemaName := fmt.Sprintf("%s.Value", sc.topicNameGenerator.SchemaTopicName(hypertable))
		hypertableSchema := schema.HypertableSchema(hypertableSchemaName, hypertable.Columns())
		sc.schemaRegistry.RegisterSchema(hypertableSchemaName, hypertableSchema)
		logger.Printf("SCHEMA UPDATE: HYPERTABLE %d => %+v", hypertable.Id(), difference)
	}
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

func (sc *SystemCatalog) initiateHypertableSchema(hypertable *model.Hypertable) error {
	if hypertable.SchemaName() == "_timescaledb_internal" ||
		hypertable.SchemaName() == "_timescaledb_catalog" {

		return nil
	}

	return sc.queryAdapter.NewSession(func(session channel.QuerySession) error {
		columns, err := sc.readHypertableSchema(session, hypertable)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		sc.ApplySchemaUpdate(hypertable, columns)
		return nil
	})
}

func (sc *SystemCatalog) readHypertableSchema(
	session channel.QuerySession, hypertable *model.Hypertable) ([]model.Column, error) {

	columns := make([]model.Column, 0)
	if err := session.QueryFunc(context.Background(), func(row pgx.Row) error {
		var name string
		var oid uint32
		var nullable, identifier bool
		var defaultValue *string

		if err := row.Scan(&name, &oid, &nullable, &identifier, &defaultValue); err != nil {
			return errors.Wrap(err, 0)
		}

		dataType, err := model.DataTypeByOID(oid)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		column := model.NewColumn(name, oid, string(dataType), nullable, identifier, defaultValue)
		columns = append(columns, column)
		return nil
	}, fmt.Sprintf(initialTableSchema, hypertable.SchemaName(), hypertable.HypertableName())); err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return columns, nil
}

func (sc *SystemCatalog) snapshotChunk(c *model.Chunk) error {
	h := sc.FindHypertableById(c.HypertableId())
	err := sc.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
		notificator.NotifyChunkSnapshotEventHandler(func(handler eventhandler.ChunkSnapshotEventHandler) error {
			return handler.OnChunkSnapshotStartedEvent(h, c)
		})
	})
	if err != nil {
		return errors.Wrap(err, 0)
	}

	if err := sc.attachChunkToPublication(c); err != nil {
		return errors.Wrap(err, 0)
	}

	go func() {
		lsn, err := sc.initiateChunkSnapshot(c, func(lsn pglogrepl.LSN, values map[string]any) error {
			return sc.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
				notificator.NotifyHypertableReplicationEventHandler(
					func(handler eventhandler.HypertableReplicationEventHandler) error {
						return handler.OnReadEvent(lsn, h, c, values)
					},
				)
			})
		})

		if err != nil {
			logger.Printf("Error while snapshotting chunk %d: %v", c.Id(), err)
		}

		if err := sc.dispatcher.EnqueueTask(func(notificator eventhandler.Notificator) {
			notificator.NotifyChunkSnapshotEventHandler(func(handler eventhandler.ChunkSnapshotEventHandler) error {
				return handler.OnChunkSnapshotFinishedEvent(h, c, lsn)
			})
		}); err != nil {
			logger.Printf("Error while notifying on snapshot finishing of chunk %d: %v", c.Id(), err)
		}
	}()
	return nil
}

func (sc *SystemCatalog) initiateChunkSnapshot(
	chunk *model.Chunk, readCallback func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error) {

	// PG internal regclass reference
	regClass := fmt.Sprintf("%s.%s", chunk.SchemaName(), chunk.TableName())

	var currentLSN pglogrepl.LSN
	err := sc.queryAdapter.NewSession(func(session channel.QuerySession) error {
		if _, err := session.Exec(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {

			return err
		}

		if err := session.QueryRow(context.Background(),
			"SELECT pg_current_wal_lsn()").Scan(&currentLSN); err != nil {

			return err
		}

		if _, err := session.Exec(context.Background(),
			fmt.Sprintf("DECLARE clone SCROLL CURSOR FOR SELECT * FROM %s", regClass)); err != nil {
			return errors.Wrap(err, 0)
		}

		for {
			count := 0
			if err := session.QueryFunc(context.Background(), func(row pgx.Row) error {
				rows := row.(pgx.Rows)

				values := make(map[string]any, len(rows.FieldDescriptions()))
				for i, field := range rows.FieldDescriptions() {
					if t, ok := sc.typeMap.TypeForOID(field.DataTypeOID); ok {
						v, err := t.Codec.DecodeValue(sc.typeMap, field.DataTypeOID, field.Format, rows.RawValues()[i])
						if err != nil {
							return errors.Wrap(err, 0)
						}
						values[field.Name] = v
					}
				}
				if err := readCallback(currentLSN, values); err != nil {
					return errors.Wrap(err, 0)
				}
				count++

				return nil
			}, "FETCH FORWARD 10 FROM clone"); err != nil {
				return errors.Wrap(err, 0)
			}
			if count == 0 {
				break
			}
		}

		_, err := session.Exec(context.Background(), "CLOSE clone")
		if err != nil {
			return errors.Wrap(err, 0)
		}

		_, err = session.Exec(context.Background(), "ROLLBACK")
		if err != nil {
			return errors.Wrap(err, 0)
		}
		return nil
	})

	if err != nil {
		return 0, errors.Wrap(err, 0)
	}

	return currentLSN, nil
}

func (sc *SystemCatalog) attachChunkToPublication(chunk *model.Chunk) error {
	chunkName := chunk.CanonicalName()
	publicationName := configuration.GetOrDefault(sc.config, "postgresql.publication", "")
	attachingQuery := fmt.Sprintf(addTableToPublication, publicationName, chunkName)

	return sc.queryAdapter.NewSession(func(session channel.QuerySession) error {
		if _, err := session.Exec(context.Background(), attachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		logger.Printf("Updated publication %s to add table %s", publicationName, chunkName)
		return nil
	})
}

/*func (sc *SystemCatalog) detachChunkToPublication(chunk *model.Chunk) error {
	chunkName := chunk.CanonicalName()
	publicationName := sc.config.PostgreSQL.Publication
	detachingQuery := fmt.Sprintf(dropTableToPublication, publicationName, chunkName)

	return sc.queryAdapter.NewSession(func(session channel.QuerySession) error {
		if _, err := session.Exec(context.Background(), detachingQuery); err != nil {
			return errors.Wrap(err, 0)
		}
		logger.Printf("Updated publication %s to drop table %s", publicationName, chunkName)
		return nil
	})
}*/

func indexOf[T comparable](slice []T, item T) int {
	for i, x := range slice {
		if x == item {
			return i
		}
	}
	return -1
}
