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

package schema

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

type Identifiable interface {
	// SchemaName returns the schema name of the entity
	SchemaName() string
	// TableName returns the table name of the entity
	TableName() string
	// CanonicalName returns the canonical name of the entity >>schema.table<<
	CanonicalName() string
}

type Buildable interface {
	SchemaBuilder() Builder
}

type TableAlike interface {
	Identifiable
	Buildable
	TableColumns() []ColumnAlike
	KeyIndexColumns() []ColumnAlike
}

type ColumnAlike interface {
	Buildable
	Name() string
	DataType() uint32
	SchemaType() Type
	IsPrimaryKey() bool
}
