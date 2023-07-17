package schemamodel

// Type is a string like definition of the available
// event stream data types
type Type string

const (
	INT8    Type = "int8"
	INT16   Type = "int16"
	INT32   Type = "int32"
	INT64   Type = "int64"
	FLOAT32 Type = "float32"
	FLOAT64 Type = "float64"
	BOOLEAN Type = "boolean"
	STRING  Type = "string"
	BYTES   Type = "bytes"
	ARRAY   Type = "array"
	MAP     Type = "map"
	STRUCT  Type = "struct"
)

type SchemaField = string

const (
	FieldNameBefore      SchemaField = "before"
	FieldNameAfter       SchemaField = "after"
	FieldNameOperation   SchemaField = "op"
	FieldNameSource      SchemaField = "source"
	FieldNameTransaction SchemaField = "transaction"
	FieldNameTimestamp   SchemaField = "ts_ms"
	FieldNameTimescaleOp SchemaField = "tsdb_op"
	FieldNameVersion     SchemaField = "version"
	FieldNameSchema      SchemaField = "schema"
	FieldNamePayload     SchemaField = "payload"
	FieldNameConnector   SchemaField = "connector"
	FieldNameName        SchemaField = "name"
	FieldNameSnapshot    SchemaField = "snapshot"
	FieldNameDatabase    SchemaField = "db"
	FieldNameSequence    SchemaField = "sequence"
	FieldNameTable       SchemaField = "table"
	FieldNameTxId        SchemaField = "txId"
	FieldNameLSN         SchemaField = "lsn"
	FieldNameXmin        SchemaField = "xmin"
	FieldNameType        SchemaField = "type"
	FieldNameOptional    SchemaField = "optional"
	FieldNameField       SchemaField = "field"
	FieldNameFields      SchemaField = "fields"
	FieldNameDefault     SchemaField = "default"
	FieldNamePrefix      SchemaField = "prefix"
	FieldNameContent     SchemaField = "content"
	FieldNameMessage     SchemaField = "message"
	FieldNameIndex       SchemaField = "index"
)

type Struct = map[SchemaField]any
