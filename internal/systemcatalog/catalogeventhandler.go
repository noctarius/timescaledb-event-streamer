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
	"fmt"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type hypertableDecomposerCallback = func(
	id int32, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16,
) error

type chunkDecomposerCallback = func(id, hypertableId int32, schemaName,
	tableName string, dropped bool, status int32, compressedChunkId *int32,
) error

type systemCatalogReplicationEventHandler struct {
	systemCatalog *systemCatalog
}

func (s *systemCatalogReplicationEventHandler) OnRelationEvent(
	_ pgtypes.XLogData, msg *pgtypes.RelationMessage,
) error {

	if msg.Namespace != "_timescaledb_catalog" {
		if hypertable, present := s.systemCatalog.FindHypertableByName(msg.Namespace, msg.RelationName); present {
			if !hypertable.IsContinuousAggregate() && msg.Namespace != "_timescaledb_internal" {
				return nil
			}

			return s.systemCatalog.sideChannel.ReadHypertableSchema(
				s.systemCatalog.ApplySchemaUpdate, s.systemCatalog.typeManager.ResolveDataType, hypertable,
			)
		}
	}
	return nil
}

func (s *systemCatalogReplicationEventHandler) OnHypertableAddedEvent(
	_ pgtypes.XLogData, _ uint32, newValues map[string]any,
) error {

	return s.decomposeHypertable(newValues,
		func(id int32, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
			compressedHypertableId *int32, compressionState int16) error {

			var viewSchema, viewName *string
			if systemcatalog.IsContinuousAggregateHypertable(hypertableName) {
				if vS, vN, found, err := s.systemCatalog.sideChannel.ReadContinuousAggregate(id); err != nil {
					return errors.Errorf("failed reading continuous aggregate information: %+v", err)
				} else if found {
					viewSchema = &vS
					viewName = &vN
				}
			}

			replicaIdentity, err := s.systemCatalog.sideChannel.ReadReplicaIdentity(schemaName, hypertableName)
			if err != nil {
				return err
			}

			h := systemcatalog.NewHypertable(
				id, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix,
				compressedHypertableId, compressionState, viewSchema, viewName, replicaIdentity,
			)

			if err := s.systemCatalog.RegisterHypertable(h); err != nil {
				return errors.Errorf("registering hypertable failed: %v (error: %+v)", h, err)
			}
			s.systemCatalog.logger.Verbosef("Entry Added: Hypertable %d => %s", h.Id(), h)

			return s.systemCatalog.sideChannel.ReadHypertableSchema(
				s.systemCatalog.ApplySchemaUpdate, s.systemCatalog.typeManager.ResolveDataType, h,
			)
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnHypertableUpdatedEvent(
	_ pgtypes.XLogData, _ uint32, _, newValues map[string]any,
) error {

	return s.decomposeHypertable(newValues,
		func(id int32, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
			compressedHypertableId *int32, compressionState int16) error {

			if hypertable, present := s.systemCatalog.FindHypertableById(id); present {
				replicaIdentity, err := s.systemCatalog.sideChannel.ReadReplicaIdentity(
					hypertable.SchemaName(), hypertable.TableName(),
				)
				if err != nil {
					return err
				}

				h, differences := hypertable.ApplyChanges(schemaName, hypertableName, associatedSchemaName,
					associatedTablePrefix, compressedHypertableId, compressionState, replicaIdentity)

				if err := s.systemCatalog.RegisterHypertable(h); err != nil {
					return errors.Errorf("registering hypertable failed: %v (error: %+v)", h, err)
				}
				s.systemCatalog.logger.Verbosef("Entry Updated: Hypertable %d => %v", id, differences)
			}
			return nil
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnHypertableDeletedEvent(
	_ pgtypes.XLogData, _ uint32, oldValues map[string]any,
) error {

	hypertableId := oldValues["id"].(int32)
	if hypertable, present := s.systemCatalog.FindHypertableById(hypertableId); present {
		if err := s.systemCatalog.UnregisterHypertable(hypertable); err != nil {
			s.systemCatalog.logger.Fatalf("unregistering hypertable failed: %s", hypertable)
		}
	}
	return nil
}

func (s *systemCatalogReplicationEventHandler) OnChunkAddedEvent(
	xld pgtypes.XLogData, _ uint32, newValues map[string]any,
) error {

	return s.decomposeChunk(newValues,
		func(id, hypertableId int32, schemaName, tableName string, dropped bool,
			status int32, compressedChunkId *int32) error {

			c := systemcatalog.NewChunk(id, hypertableId, schemaName, tableName, dropped, status, compressedChunkId)
			if err := s.systemCatalog.RegisterChunk(c); err != nil {
				return errors.Errorf("registering chunk failed: %v (error: %+v)", c, err)
			}

			if h, present := s.systemCatalog.FindHypertableById(hypertableId); present {
				s.systemCatalog.logger.Verbosef(
					"Entry Added: Chunk %d for Hypertable %s => %s",
					c.Id(), h.CanonicalName(), c,
				)

				if !c.IsCompressed() &&
					s.systemCatalog.IsHypertableSelectedForReplication(hypertableId) {

					publicationManager := s.systemCatalog.publicationManager
					if found, err := publicationManager.ExistsTableInPublication(c); err != nil {
						return err
					} else if found {
						s.systemCatalog.logger.Infof(
							"Chunk %s already in publication %s but seems to "+
								"be created while offline, forcing snapshotting - this may lead to duplicates!",
							c.CanonicalName(), publicationManager.PublicationName(),
						)
					}
					if err := s.systemCatalog.snapshotChunkWithXld(&xld, c); err != nil {
						s.systemCatalog.logger.Fatalf("failed to snapshot chunk %s", c.CanonicalName())
					}
				}
			}

			return nil
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnChunkUpdatedEvent(
	_ pgtypes.XLogData, _ uint32, _, newValues map[string]any,
) error {

	return s.decomposeChunk(newValues,
		func(id, hypertableId int32, schemaName, tableName string, dropped bool,
			status int32, compressedChunkId *int32) error {

			if chunk, present := s.systemCatalog.FindChunkById(id); present {
				c, differences := chunk.ApplyChanges(schemaName, tableName, dropped, status, compressedChunkId)

				hypertableName := "unknown"
				if uH, cH, present := s.systemCatalog.ResolveUncompressedHypertable(hypertableId); present {
					hypertableName = uH.CanonicalName()
					if cH != nil {
						hypertableName = fmt.Sprintf(
							"%s VIA %s", hypertableName, cH.CanonicalName())
					}
				}

				if err := s.systemCatalog.RegisterChunk(c); err != nil {
					return errors.Errorf("registering chunk failed: %v (error: %+v)", c, err)
				}
				if c.Dropped() && !chunk.Dropped() {
					s.systemCatalog.logger.Verbosef("Entry Dropped: Chunk %d for Hypertable %s => %v",
						id, hypertableName, differences)
				} else {
					s.systemCatalog.logger.Verbosef(
						"Entry Updated: Chunk %d for Hypertable %s => %v",
						id, hypertableName, differences)
				}
			}
			return nil
		},
	)
}

func (s *systemCatalogReplicationEventHandler) OnChunkDeletedEvent(
	_ pgtypes.XLogData, _ uint32, oldValues map[string]any,
) error {

	chunkId := oldValues["id"].(int32)
	if chunk, present := s.systemCatalog.FindChunkById(chunkId); present {
		hypertableName := "unknown"
		if uH, cH, present := s.systemCatalog.ResolveUncompressedHypertable(chunk.HypertableId()); present {
			hypertableName = fmt.Sprintf("%s.%s", uH.SchemaName(), uH.TableName())
			if cH != nil {
				hypertableName = fmt.Sprintf(
					"%s VIA %s.%s", cH.SchemaName(), cH.TableName(), hypertableName)
			}
		}

		err := s.systemCatalog.UnregisterChunk(chunk)
		if err != nil {
			s.systemCatalog.logger.Fatalf("detaching chunk failed: %d => %v", chunkId, err)
		}
		s.systemCatalog.logger.Verbosef("Entry Dropped: Chunk %d for Hypertable %s", chunkId, hypertableName)
	}
	return nil
}

func (s *systemCatalogReplicationEventHandler) decomposeHypertable(
	values map[string]any, cb hypertableDecomposerCallback,
) error {

	id := values["id"].(int32)
	schemaName := values["schema_name"].(string)
	hypertableName := values["table_name"].(string)
	associatedSchemaName := values["associated_schema_name"].(string)
	associatedTablePrefix := values["associated_table_prefix"].(string)
	compressionState := values["compression_state"].(int16)
	var compressedHypertableId *int32
	if v, ok := values["compressed_hypertable_id"].(int32); ok {
		compressedHypertableId = &v
	}
	return cb(id, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix,
		compressedHypertableId, compressionState,
	)
}

func (s *systemCatalogReplicationEventHandler) decomposeChunk(
	values map[string]any, cb chunkDecomposerCallback,
) error {

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
