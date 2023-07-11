package schemamodel

// SchemaType is a string like definition of the available
// event stream data types
type SchemaType string

const (
	INT8    SchemaType = "int8"
	INT16   SchemaType = "int16"
	INT32   SchemaType = "int32"
	INT64   SchemaType = "int64"
	FLOAT32 SchemaType = "float32"
	FLOAT64 SchemaType = "float64"
	BOOLEAN SchemaType = "boolean"
	STRING  SchemaType = "string"
	BYTES   SchemaType = "bytes"
	ARRAY   SchemaType = "array"
	MAP     SchemaType = "map"
	STRUCT  SchemaType = "struct"
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
