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
	"strings"
)

type Chunk struct {
	*baseSystemEntity
	id                int32
	hypertableId      int32
	compressedChunkId *int32
	dropped           bool
	status            int32
	compressed        bool
}

func NewChunk(
	id, hypertableId int32, schemaName, tableName string, dropped bool, status int32, compressedChunkId *int32,
) *Chunk {

	return &Chunk{
		baseSystemEntity: &baseSystemEntity{
			schemaName: schemaName,
			tableName:  tableName,
		},
		id:                id,
		hypertableId:      hypertableId,
		compressedChunkId: compressedChunkId,
		dropped:           dropped,
		status:            status,
		compressed:        strings.HasPrefix(tableName, "compress_"),
	}
}

func (c *Chunk) Id() int32 {
	return c.id
}

func (c *Chunk) HypertableId() int32 {
	return c.hypertableId
}

func (c *Chunk) CompressedChunkId() *int32 {
	return c.compressedChunkId
}

func (c *Chunk) Dropped() bool {
	return c.dropped
}

func (c *Chunk) Status() int32 {
	return c.status
}

func (c *Chunk) IsPartiallyCompressed() bool {
	return c.status&0x08 == 0x08
}

func (c *Chunk) IsCompressed() bool {
	return c.compressed
}

func (c *Chunk) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("id:%d ", c.id))
	builder.WriteString(fmt.Sprintf("hypertableId:%d ", c.hypertableId))
	builder.WriteString(fmt.Sprintf("schemaName:%s ", c.schemaName))
	builder.WriteString(fmt.Sprintf("tableName:%s ", c.tableName))
	if c.compressedChunkId == nil {
		builder.WriteString("compressedChunkId:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("compressedChunkId:%d ", *c.compressedChunkId))
	}
	builder.WriteString(fmt.Sprintf("dropped:%t ", c.dropped))
	builder.WriteString(fmt.Sprintf("status:%d", c.status))
	builder.WriteString("}")
	return builder.String()
}

func (c *Chunk) ApplyChanges(
	schemaName, tableName string, dropped bool, status int32, compressedChunkId *int32,
) (*Chunk, map[string]string) {

	c2 := &Chunk{
		baseSystemEntity: &baseSystemEntity{
			schemaName: schemaName,
			tableName:  tableName,
		},
		id:                c.id,
		hypertableId:      c.hypertableId,
		compressedChunkId: compressedChunkId,
		dropped:           dropped,
		status:            status,
	}
	return c2, c.differences(c2)
}

func (c *Chunk) differences(
	new *Chunk,
) map[string]string {

	differences := make(map[string]string, 0)
	if c.id != new.id {
		differences["id"] = fmt.Sprintf("%d=>%d", c.id, new.id)
	}
	if c.hypertableId != new.hypertableId {
		differences["hypertableId"] = fmt.Sprintf("%d=>%d", c.hypertableId, new.hypertableId)
	}
	if c.schemaName != new.schemaName {
		differences["schemaName"] = fmt.Sprintf("%s=>%s", c.schemaName, new.schemaName)
	}
	if c.tableName != new.tableName {
		differences["tableName"] = fmt.Sprintf("%s=>%s", c.tableName, new.tableName)
	}
	if (c.compressedChunkId == nil && new.compressedChunkId != nil) ||
		(c.compressedChunkId != nil && new.compressedChunkId == nil) {
		o := "<nil>"
		if c.compressedChunkId != nil {
			o = fmt.Sprintf("%d", *c.compressedChunkId)
		}
		n := "<nil>"
		if new.compressedChunkId != nil {
			n = fmt.Sprintf("%d", *new.compressedChunkId)
		}
		differences["compressedChunkId"] = fmt.Sprintf("%s=>%s", o, n)
	}
	if c.dropped != new.dropped {
		differences["dropped"] = fmt.Sprintf("%t=>%t", c.dropped, new.dropped)
	}
	if c.status != new.status {
		differences["status"] = fmt.Sprintf("%d=>%d", c.status, new.status)
	}
	return differences
}
