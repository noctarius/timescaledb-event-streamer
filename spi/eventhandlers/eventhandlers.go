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

package eventhandlers

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type BaseReplicationEventHandler interface {
	OnRelationEvent(
		xld pgtypes.XLogData, msg *pgtypes.RelationMessage,
	) error
}

type LogicalReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnBeginEvent(
		xld pgtypes.XLogData, msg *pgtypes.BeginMessage,
	) error
	OnCommitEvent(
		xld pgtypes.XLogData, msg *pgtypes.CommitMessage,
	) error
	OnInsertEvent(
		xld pgtypes.XLogData, msg *pgtypes.InsertMessage,
	) error
	OnUpdateEvent(
		xld pgtypes.XLogData, msg *pgtypes.UpdateMessage,
	) error
	OnDeleteEvent(
		xld pgtypes.XLogData, msg *pgtypes.DeleteMessage,
	) error
	OnTruncateEvent(
		xld pgtypes.XLogData, msg *pgtypes.TruncateMessage,
	) error
	OnTypeEvent(
		xld pgtypes.XLogData, msg *pgtypes.TypeMessage,
	) error
	OnOriginEvent(
		xld pgtypes.XLogData, msg *pgtypes.OriginMessage,
	) error
	OnMessageEvent(
		xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage,
	) error
}

type RecordReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnReadEvent(
		lsn pgtypes.LSN, table schema.TableAlike,
		chunk *systemcatalog.Chunk, newValues map[string]any,
	) error
	OnInsertEvent(
		xld pgtypes.XLogData, table schema.TableAlike,
		chunk *systemcatalog.Chunk, newValues map[string]any,
	) error
	OnUpdateEvent(
		xld pgtypes.XLogData, table schema.TableAlike,
		chunk *systemcatalog.Chunk, oldValues, newValues map[string]any,
	) error
	OnDeleteEvent(
		xld pgtypes.XLogData, table schema.TableAlike,
		chunk *systemcatalog.Chunk, oldValues map[string]any, tombstone bool,
	) error
	OnTruncateEvent(
		xld pgtypes.XLogData, table schema.TableAlike,
	) error
	OnBeginEvent(
		xld pgtypes.XLogData, msg *pgtypes.BeginMessage,
	) error
	OnCommitEvent(
		xld pgtypes.XLogData, msg *pgtypes.CommitMessage,
	) error
	OnTypeEvent(
		xld pgtypes.XLogData, msg *pgtypes.TypeMessage,
	) error
	OnOriginEvent(
		xld pgtypes.XLogData, msg *pgtypes.OriginMessage,
	) error
	OnMessageEvent(
		xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage,
	) error
	OnTransactionFinishedEvent(
		xld pgtypes.XLogData, msg *pgtypes.CommitMessage,
	) error
}

type SnapshottingEventHandler interface {
	BaseReplicationEventHandler
	OnChunkSnapshotStartedEvent(
		hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
	) error
	OnChunkSnapshotFinishedEvent(
		hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk, snapshot pgtypes.LSN,
	) error
	OnTableSnapshotStartedEvent(
		snapshotName string, table systemcatalog.BaseTable,
	) error
	OnTableSnapshotFinishedEvent(
		snapshotName string, table systemcatalog.BaseTable, lsn pgtypes.LSN,
	) error
	OnSnapshottingStartedEvent(
		snapshotName string,
	) error
	OnSnapshottingFinishedEvent() error
}

type CompressionReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnChunkCompressedEvent(
		xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
	) error
	OnChunkDecompressedEvent(
		xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, chunk *systemcatalog.Chunk,
	) error
}

type SystemCatalogReplicationEventHandler interface {
	BaseReplicationEventHandler
	OnHypertableAddedEvent(
		xld pgtypes.XLogData, relationId uint32, newValues map[string]any,
	) error
	OnHypertableUpdatedEvent(
		xld pgtypes.XLogData, relationId uint32, oldValues, newValues map[string]any,
	) error
	OnHypertableDeletedEvent(
		xld pgtypes.XLogData, relationId uint32, oldValues map[string]any,
	) error
	OnChunkAddedEvent(
		xld pgtypes.XLogData, relationId uint32, newValues map[string]any,
	) error
	OnChunkUpdatedEvent(
		xld pgtypes.XLogData, relationId uint32, oldValues, newValues map[string]any,
	) error
	OnChunkDeletedEvent(
		xld pgtypes.XLogData, relationId uint32, oldValues map[string]any,
	) error
}
