package systemcatalog

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
)

type hypertableDecomposerCallback = func(
	id int32, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16, distributed bool) error

type chunkDecomposerCallback = func(
	id, hypertableId int32, schemaName, tableName string, dropped bool, status int32, compressedChunkId *int32) error

type systemCatalogReplicationEventHandler struct {
	systemCatalog *SystemCatalog
}

func (s *systemCatalogReplicationEventHandler) OnRelationEvent(
	_ pglogrepl.XLogData, msg *pglogrepl.RelationMessage) error {

	if msg.Namespace != "_timescaledb_internal" && msg.Namespace != "_timescaledb_catalog" {
		hypertable := s.systemCatalog.FindHypertableByName(msg.Namespace, msg.RelationName)
		columns := make([]model.Column, len(msg.Columns))
		for i, c := range msg.Columns {
			dataType, err := model.DataTypeByOID(c.DataType)
			if err != nil {
				return err
			}

			columns[i] = model.NewColumn(c.Name, c.DataType, string(dataType), false, false, nil)
		}
		s.systemCatalog.ApplySchemaUpdate(hypertable, columns)
	}
	return nil
}

func (s *systemCatalogReplicationEventHandler) OnHypertableAddedEvent(_ uint32, newValues map[string]any) error {
	return s.decomposeHypertable(newValues,
		func(id int32, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
			compressedHypertableId *int32, compressionState int16, distributed bool) error {

			h := model.NewHypertable(id, s.systemCatalog.databaseName, schemaName, hypertableName,
				associatedSchemaName, associatedTablePrefix, compressedHypertableId, compressionState, distributed)

			if err := s.systemCatalog.RegisterHypertable(h); err != nil {
				return fmt.Errorf("registering hypertable failed: %v", h)
			}
			logger.Printf("ADDED CATALOG ENTRY: HYPERTABLE %d => %+v", id, *h)

			return s.systemCatalog.sideChannel.ReadHypertableSchema(h, s.systemCatalog.ApplySchemaUpdate)
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnHypertableUpdatedEvent(_ uint32, _, newValues map[string]any) error {
	return s.decomposeHypertable(newValues,
		func(id int32, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
			compressedHypertableId *int32, compressionState int16, distributed bool) error {

			hypertable := s.systemCatalog.FindHypertableById(id)
			if hypertable != nil {
				h, differences := hypertable.ApplyChanges(schemaName, hypertableName, associatedSchemaName,
					associatedTablePrefix, compressedHypertableId, compressionState)

				if err := s.systemCatalog.RegisterHypertable(h); err != nil {
					return fmt.Errorf("registering hypertable failed: %v", h)
				}
				logger.Printf("UPDATED CATALOG ENTRY: HYPERTABLE %d => %v", id, differences)
			}
			return nil
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnHypertableDeletedEvent(_ uint32, oldValues map[string]any) error {
	hypertableId := oldValues["id"].(int32)
	if hypertable := s.systemCatalog.FindHypertableById(hypertableId); hypertable != nil {
		if err := s.systemCatalog.UnregisterHypertable(hypertable); err != nil {
			logger.Fatalf("unregistering hypertable failed: %v", hypertable)
		}
	}
	logger.Printf("REMOVED CATALOG ENTRY: HYPERTABLE %d", hypertableId)
	return nil
}

func (s *systemCatalogReplicationEventHandler) OnChunkAddedEvent(_ uint32, newValues map[string]any) error {
	return s.decomposeChunk(newValues,
		func(id, hypertableId int32, schemaName, tableName string, dropped bool,
			status int32, compressedChunkId *int32) error {

			hypertableName := "unknown"
			if h := s.systemCatalog.FindHypertableById(hypertableId); h != nil {
				hypertableName = fmt.Sprintf("%s.%s", h.SchemaName(), h.HypertableName())
				if h.IsCompressedTable() {
					h = s.systemCatalog.FindHypertableByCompressedHypertableId(h.Id())
					hypertableName = fmt.Sprintf(
						"%s.%s VIA %s", h.SchemaName(), h.HypertableName(), hypertableName)
				}
			}

			c := model.NewChunk(id, hypertableId, schemaName, tableName, dropped, status, compressedChunkId)
			if err := s.systemCatalog.RegisterChunk(c); err != nil {
				return fmt.Errorf("registering chunk failed: %v", c)
			}
			logger.Printf("ADDED CATALOG ENTRY: CHUNK %d FOR HYPERTABLE %s => %+v", id, hypertableName, *c)

			if !c.IsCompressed() &&
				s.systemCatalog.IsHypertableSelectedForReplication(hypertableId) {

				go func() {
					if err := s.systemCatalog.snapshotChunk(c); err != nil {
						logger.Fatalf("failed to snapshot chunk %s", c.CanonicalName())
					}
				}()
			}

			return nil
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnChunkUpdatedEvent(_ uint32, _, newValues map[string]any) error {
	return s.decomposeChunk(newValues,
		func(id, hypertableId int32, schemaName, tableName string, dropped bool,
			status int32, compressedChunkId *int32) error {

			chunk := s.systemCatalog.FindChunkById(id)
			c, differences := chunk.ApplyChanges(schemaName, tableName, dropped, status, compressedChunkId)

			hypertableName := "unknown"
			if h := s.systemCatalog.FindHypertableById(hypertableId); h != nil {
				hypertableName = fmt.Sprintf("%s.%s", h.SchemaName(), h.HypertableName())
				if h.IsCompressedTable() {
					h = s.systemCatalog.FindHypertableByCompressedHypertableId(h.Id())
					hypertableName = fmt.Sprintf(
						"%s.%s VIA %s", h.SchemaName(), h.HypertableName(), hypertableName)
				}
			}

			if err := s.systemCatalog.RegisterChunk(c); err != nil {
				return fmt.Errorf("registering chunk failed: %v", c)
			}
			if c.Dropped() && !chunk.Dropped() {
				logger.Printf("UPDATED CATALOG ENTRY: CHUNK %d DROPPED FOR HYPERTABLE %s => %v",
					id, hypertableName, differences)
			} else {
				logger.Printf(
					"UPDATED CATALOG ENTRY: CHUNK %d FOR HYPERTABLE %s => %v",
					id, hypertableName, differences)
			}
			return nil
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnChunkDeletedEvent(_ uint32, oldValues map[string]any) error {
	chunkId := oldValues["id"].(int32)
	chunk := s.systemCatalog.FindChunkById(chunkId)
	if chunk == nil {
		return nil
	}

	hypertableName := "unknown"
	if h := s.systemCatalog.FindHypertableById(chunk.HypertableId()); h != nil {
		hypertableName = fmt.Sprintf("%s.%s", h.SchemaName(), h.HypertableName())
		if h.IsCompressedTable() {
			h = s.systemCatalog.FindHypertableByCompressedHypertableId(h.Id())
			hypertableName = fmt.Sprintf(
				"%s.%s VIA %s", h.SchemaName(), h.HypertableName(), hypertableName)
		}
	}

	err := s.systemCatalog.UnregisterChunk(chunk)
	if err != nil {
		logger.Fatalf("detaching chunk failed: %d => %v", chunkId, err)
	}
	logger.Printf("REMOVED CATALOG ENTRY: CHUNK %d FOR HYPERTABLE %s", chunkId, hypertableName)
	return nil
}

func (s *systemCatalogReplicationEventHandler) decomposeHypertable(values map[string]any,
	cb hypertableDecomposerCallback) error {

	id := values["id"].(int32)
	schemaName := values["schema_name"].(string)
	hypertableName := values["table_name"].(string)
	associatedSchemaName := values["associated_schema_name"].(string)
	associatedTablePrefix := values["associated_table_prefix"].(string)
	compressionState := values["compression_state"].(int16)
	var distributed bool
	if v, ok := values["replication_factor"].(int16); ok {
		distributed = v > 0
	}
	var compressedHypertableId *int32
	if v, ok := values["compressed_hypertable_id"].(int32); ok {
		compressedHypertableId = &v
	}
	return cb(id, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix,
		compressedHypertableId, compressionState, distributed,
	)
}

func (s *systemCatalogReplicationEventHandler) decomposeChunk(values map[string]any,
	cb chunkDecomposerCallback) error {

	id := values["id"].(int32)
	hypertableId := values["hypertable_id"].(int32)
	schemaName := values["schema_name"].(string)
	tableName := values["table_name"].(string)
	dropped := values["dropped"].(bool)
	status := values["status"].(int32)

	var compressedChunkId *int32
	if v, ok := values["compressed_chunk_id"].(int32); ok {
		compressedChunkId = &v
	}

	return cb(id, hypertableId, schemaName, tableName, dropped, status, compressedChunkId)
}
