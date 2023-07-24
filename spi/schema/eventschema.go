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

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/samber/lo"
	"time"
)

const SourceSchemaName = "io.debezium.connector.postgresql.Source"
const MessageBlockSchemaName = "io.debezium.connector.postgresql.Message"
const MessageKeySchemaName = "io.debezium.connector.postgresql.MessageKey"
const MessageValueSchemaName = "io.debezium.connector.postgresql.MessageValue"
const TimescaleEventSchemaName = "com.timescale.Event"

type Operation string

const (
	OP_READ      Operation = "r"
	OP_CREATE    Operation = "c"
	OP_UPDATE    Operation = "u"
	OP_DELETE    Operation = "d"
	OP_TRUNCATE  Operation = "t"
	OP_MESSAGE   Operation = "m"
	OP_TIMESCALE Operation = "$"
)

type TimescaleOperation string

const (
	OP_COMPRESSION   TimescaleOperation = "c"
	OP_DECOMPRESSION TimescaleOperation = "d"
)

func ReadEvent(
	record Struct, source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_READ)
	event[FieldNameAfter] = record
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CreateEvent(
	record Struct, source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_CREATE)
	event[FieldNameAfter] = record
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func UpdateEvent(
	before, after, source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_UPDATE)
	if before != nil {
		event[FieldNameBefore] = before
	}
	if after != nil {
		event[FieldNameAfter] = after
	}
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DeleteEvent(
	before, source Struct, tombstone bool,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_DELETE)
	if before != nil {
		event[FieldNameBefore] = before
	}
	if tombstone {
		event[FieldNameAfter] = nil
	}
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func TruncateEvent(
	source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_TRUNCATE)
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func MessageEvent(
	prefix string, content *string, source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_MESSAGE)
	block := Struct{
		FieldNamePrefix: prefix,
	}
	if content != nil {
		block[FieldNameContent] = *content
	}
	event[FieldNameMessage] = block
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CompressionEvent(
	source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_TIMESCALE)
	event[FieldNameTimescaleOp] = string(OP_COMPRESSION)
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DecompressionEvent(
	source Struct,
) Struct {

	event := make(Struct)
	event[FieldNameOperation] = string(OP_TIMESCALE)
	event[FieldNameTimescaleOp] = string(OP_DECOMPRESSION)
	if source != nil {
		event[FieldNameSource] = source
	}
	event[FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func MessageKey(
	prefix string,
) Struct {

	return Struct{
		FieldNamePrefix: prefix,
	}
}

func TimescaleKey(
	schemaName, tableName string,
) Struct {

	return Struct{
		FieldNameSchema: schemaName,
		FieldNameTable:  tableName,
	}
}

func Envelope(
	schema, payload Struct,
) Struct {

	return Struct{
		FieldNameSchema:  schema,
		FieldNamePayload: payload,
	}
}

func Source(
	lsn pglogrepl.LSN, timestamp time.Time, snapshot bool,
	databaseName, schemaName, hypertableName string, transactionId *uint32,
) Struct {

	return Struct{
		FieldNameVersion:   version.Version,
		FieldNameConnector: "timescaledb-event-streamer",
		FieldNameName:      databaseName,
		FieldNameTimestamp: timestamp.UnixMilli(),
		FieldNameSnapshot:  snapshot,
		FieldNameDatabase:  databaseName,
		FieldNameSchema:    schemaName,
		FieldNameTable:     hypertableName,
		FieldNameTxId:      transactionId,
		FieldNameLSN:       lsn.String(),
	}
}

func KeySchema(
	nameGenerator NameGenerator, table TableAlike,
) Struct {

	schemaTopicName := nameGenerator.SchemaTopicName(table)
	hypertableKeySchemaName := fmt.Sprintf("%s.Key", schemaTopicName)

	return Struct{
		FieldNameType:     string(STRUCT),
		FieldNameName:     hypertableKeySchemaName,
		FieldNameOptional: false,
		FieldNameFields: func() []Struct {
			keys := make([]Struct, 0)
			fieldIndex := 0
			for _, column := range table.TableColumns() {
				if !column.IsPrimaryKey() {
					continue
				}
				keys = append(keys, keySchemaElement(column.Name(), fieldIndex, column.SchemaType(), false))
				fieldIndex++
			}
			return keys
		}(),
	}
}

func TimescaleEventKeySchema() Struct {
	return Struct{
		FieldNameType:     string(STRUCT),
		FieldNameName:     TimescaleEventSchemaName,
		FieldNameOptional: false,
		FieldNameFields: []Struct{
			simpleSchemaElement(FieldNameSchema, STRING, false),
			simpleSchemaElement(FieldNameTable, STRING, false),
		},
	}
}

func EnvelopeSchema(
	nameGenerator NameGenerator, table TableAlike,
) Struct {

	schemaTopicName := nameGenerator.SchemaTopicName(table)
	hypertableSchemaName := fmt.Sprintf("%s.Value", schemaTopicName)
	envelopeSchemaName := fmt.Sprintf("%s.Envelope", schemaTopicName)
	hypertableSchema := table.SchemaBuilder().SchemaName(hypertableSchemaName)

	return NewSchemaBuilder(STRUCT).
		SchemaName(envelopeSchemaName).
		Required().
		Field(FieldNameBefore, -1, hypertableSchema.Clone()).
		Field(FieldNameAfter, -1, hypertableSchema.Clone().Required()).
		Field(FieldNameSource, -1, SourceSchema()).
		Field(FieldNameOperation, -1, String().Required()).
		Field(FieldNameTimescaleOp, -1, String()).
		Field(FieldNameTimestamp, -1, Int64()).
		Build()
}

func EnvelopeMessageSchema(
	nameGenerator NameGenerator,
) Struct {

	schemaTopicName := nameGenerator.MessageTopicName()
	envelopeSchemaName := fmt.Sprintf("%s.Envelope", schemaTopicName)

	return Struct{
		FieldNameType: string(STRUCT),
		FieldNameFields: []Struct{
			MessageValueSchema(),
			SourceSchema().Build(),
			simpleSchemaElement(FieldNameOperation, STRING, false),
			simpleSchemaElement(FieldNameTimescaleOp, STRING, true),
			simpleSchemaElement(FieldNameTimestamp, INT64, true),
		},
		FieldNameOptional: false,
		FieldNameName:     envelopeSchemaName,
	}
}

func SourceSchema() Builder {
	return NewSchemaBuilder(STRUCT).
		FieldName(FieldNameSource).
		SchemaName(SourceSchemaName).
		Required().
		Field(FieldNameVersion, -1, String().Required()).
		Field(FieldNameConnector, -1, String().Required()).
		Field(FieldNameName, -1, String().Required()).
		Field(FieldNameTimestamp, -1, String().Required()).
		Field(FieldNameSnapshot, -1, Boolean().DefaultValue(lo.ToPtr("false"))).
		Field(FieldNameSchema, -1, String().Required()).
		Field(FieldNameTable, -1, String().Required()).
		Field(FieldNameTxId, -1, Int64()).
		Field(FieldNameLSN, -1, Int64()).
		Field(FieldNameXmin, -1, Int64())
}

func MessageValueSchema() Struct {
	return Struct{
		FieldNameVersion: 1,
		FieldNameName:    MessageValueSchemaName,
		FieldNameFields: []Struct{
			simpleSchemaElement(FieldNameOperation, STRING, false),
			simpleSchemaElement(FieldNameTimestamp, INT64, true),
			SourceSchema().Build(),
			{
				FieldNameField:    FieldNameMessage,
				FieldNameOptional: false,
				FieldNameMessage:  messageBlockSchema(),
			},
		},
	}
}

func MessageKeySchema() Struct {
	return Struct{
		FieldNameVersion: 1,
		FieldNameName:    MessageKeySchemaName,
		FieldNameFields: []Struct{
			simpleSchemaElement(FieldNamePrefix, STRING, true),
		},
	}
}

func messageBlockSchema() Struct {
	return Struct{
		FieldNameVersion: 1,
		FieldNameName:    MessageBlockSchemaName,
		FieldNameFields: []Struct{
			simpleSchemaElement(FieldNamePrefix, STRING, false),
			simpleSchemaElement(FieldNameContent, STRING, true),
		},
	}
}

func simpleSchemaElement(
	fieldName FieldName, schemaType Type, optional bool,
) Struct {

	return Struct{
		FieldNameType:     string(schemaType),
		FieldNameOptional: optional,
		FieldNameField:    fieldName,
	}
}

func keySchemaElement(
	fieldName FieldName, index int, schemaType Type, optional bool,
) Struct {

	return Struct{
		FieldNameName:  fieldName,
		FieldNameIndex: index,
		FieldNameSchema: Struct{
			FieldNameType:     string(schemaType),
			FieldNameOptional: optional,
		},
	}
}
