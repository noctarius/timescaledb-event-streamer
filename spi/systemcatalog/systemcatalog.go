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

type SystemCatalog interface {
	FindHypertableById(
		hypertableId int32,
	) (hypertable *Hypertable, present bool)

	FindHypertableByName(
		schema, name string,
	) (hypertable *Hypertable, present bool)

	FindHypertableByChunkId(
		chunkId int32,
	) (hypertable *Hypertable, present bool)

	FindHypertableByCompressedHypertableId(
		compressedHypertableId int32,
	) (hypertable *Hypertable, present bool)

	FindCompressedHypertableByHypertableId(
		hypertableId int32,
	) (hypertable *Hypertable, present bool)

	FindChunkById(
		id int32,
	) (chunk *Chunk, present bool)

	FindChunkByName(
		schemaName, tableName string,
	) (chunk *Chunk, present bool)

	ResolveOriginHypertable(
		chunk *Chunk,
	) (hypertable *Hypertable, present bool)

	ResolveUncompressedHypertable(
		hypertableId int32,
	) (uncompressedHypertable, compressedHypertable *Hypertable, present bool)

	IsHypertableSelectedForReplication(
		hypertableId int32,
	) bool

	RegisterHypertable(
		hypertable *Hypertable,
	) error

	UnregisterHypertable(
		hypertable *Hypertable,
	) error

	RegisterChunk(
		chunk *Chunk,
	) error

	UnregisterChunk(
		chunk *Chunk,
	) error

	ApplySchemaUpdate(
		hypertable *Hypertable, columns []Column,
	) bool

	GetAllChunks() []SystemEntity
}
