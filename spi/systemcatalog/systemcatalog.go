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
