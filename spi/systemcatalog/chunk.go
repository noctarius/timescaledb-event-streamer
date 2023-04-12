package systemcatalog

import (
	"fmt"
	"strings"
)

type Chunk struct {
	id                int32
	hypertableId      int32
	schemaName        string
	tableName         string
	compressedChunkId *int32
	dropped           bool
	status            int32
	compressed        bool
}

func NewChunk(id, hypertableId int32, schemaName, tableName string, dropped bool,
	status int32, compressedChunkId *int32) *Chunk {

	return &Chunk{
		id:                id,
		hypertableId:      hypertableId,
		schemaName:        schemaName,
		tableName:         tableName,
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

func (c *Chunk) SchemaName() string {
	return c.schemaName
}

func (c *Chunk) TableName() string {
	return c.tableName
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

func (c *Chunk) CanonicalName() string {
	return canonicalChunkName(c)
}

func (c *Chunk) ApplyChanges(schemaName, tableName string, dropped bool,
	status int32, compressedChunkId *int32) (*Chunk, map[string]string) {

	c2 := &Chunk{
		id:                c.id,
		hypertableId:      c.hypertableId,
		schemaName:        schemaName,
		tableName:         tableName,
		compressedChunkId: compressedChunkId,
		dropped:           dropped,
		status:            status,
	}
	return c2, c.differences(c2)
}

func (c *Chunk) differences(new *Chunk) map[string]string {
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
