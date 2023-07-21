package schemamodel

// Type is a string like definition of the available
// event stream data types
type Type string

func (st Type) IsPrimitive() bool {
	switch st {
	case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES:
		return true
	}
	return false
}

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

type FieldName = string

const (
	FieldNameBefore      FieldName = "before"
	FieldNameAfter       FieldName = "after"
	FieldNameOperation   FieldName = "op"
	FieldNameSource      FieldName = "source"
	FieldNameTransaction FieldName = "transaction"
	FieldNameTimestamp   FieldName = "ts_ms"
	FieldNameTimescaleOp FieldName = "tsdb_op"
	FieldNameVersion     FieldName = "version"
	FieldNameSchema      FieldName = "schema"
	FieldNamePayload     FieldName = "payload"
	FieldNameConnector   FieldName = "connector"
	FieldNameName        FieldName = "name"
	FieldNameSnapshot    FieldName = "snapshot"
	FieldNameDatabase    FieldName = "db"
	FieldNameSequence    FieldName = "sequence"
	FieldNameTable       FieldName = "table"
	FieldNameTxId        FieldName = "txId"
	FieldNameLSN         FieldName = "lsn"
	FieldNameXmin        FieldName = "xmin"
	FieldNameType        FieldName = "type"
	FieldNameOptional    FieldName = "optional"
	FieldNameField       FieldName = "field"
	FieldNameFields      FieldName = "fields"
	FieldNameDefault     FieldName = "default"
	FieldNamePrefix      FieldName = "prefix"
	FieldNameContent     FieldName = "content"
	FieldNameMessage     FieldName = "message"
	FieldNameIndex       FieldName = "index"
	FieldNameKeySchema   FieldName = "keySchema"
	FieldNameValueSchema FieldName = "valueSchema"
	FieldNameAllowed     FieldName = "allowed"
	FieldNameLength      FieldName = "length"
)

type Struct = map[FieldName]any
