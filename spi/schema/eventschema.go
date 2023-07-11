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
	"github.com/noctarius/timescaledb-event-streamer/internal/version"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namegenerator"
	"strconv"
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

func ReadEvent(record schemamodel.Struct, source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_READ)
	event[schemamodel.FieldNameAfter] = record
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CreateEvent(record schemamodel.Struct, source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_CREATE)
	event[schemamodel.FieldNameAfter] = record
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func UpdateEvent(before, after, source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_UPDATE)
	if before != nil {
		event[schemamodel.FieldNameBefore] = before
	}
	if after != nil {
		event[schemamodel.FieldNameAfter] = after
	}
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DeleteEvent(before, source schemamodel.Struct, tombstone bool) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_DELETE)
	if before != nil {
		event[schemamodel.FieldNameBefore] = before
	}
	if tombstone {
		event[schemamodel.FieldNameAfter] = nil
	}
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func TruncateEvent(source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_TRUNCATE)
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func MessageEvent(prefix, content string, source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_MESSAGE)
	event[schemamodel.FieldNameMessage] = schemamodel.Struct{
		schemamodel.FieldNamePrefix:  prefix,
		schemamodel.FieldNameContent: content,
	}
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CompressionEvent(source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_TIMESCALE)
	event[schemamodel.FieldNameTimescaleOp] = string(OP_COMPRESSION)
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DecompressionEvent(source schemamodel.Struct) schemamodel.Struct {
	event := make(schemamodel.Struct, 0)
	event[schemamodel.FieldNameOperation] = string(OP_TIMESCALE)
	event[schemamodel.FieldNameTimescaleOp] = string(OP_DECOMPRESSION)
	if source != nil {
		event[schemamodel.FieldNameSource] = source
	}
	event[schemamodel.FieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func MessageKey(prefix string) schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNamePrefix: prefix,
	}
}

func TimescaleKey(schemaName, tableName string) schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameSchema: schemaName,
		schemamodel.FieldNameTable:  tableName,
	}
}

func Envelope(schema, payload schemamodel.Struct) schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameSchema:  schema,
		schemamodel.FieldNamePayload: payload,
	}
}

func Source(lsn pglogrepl.LSN, timestamp time.Time, snapshot bool,
	databaseName, schemaName, hypertableName string, transactionId *uint32) schemamodel.Struct {

	return schemamodel.Struct{
		schemamodel.FieldNameVersion:   version.Version,
		schemamodel.FieldNameConnector: "timescaledb-event-streamer",
		schemamodel.FieldNameName:      databaseName,
		schemamodel.FieldNameTimestamp: timestamp.UnixMilli(),
		schemamodel.FieldNameSnapshot:  snapshot,
		schemamodel.FieldNameDatabase:  databaseName,
		schemamodel.FieldNameSchema:    schemaName,
		schemamodel.FieldNameTable:     hypertableName,
		schemamodel.FieldNameTxId:      transactionId,
		schemamodel.FieldNameLSN:       lsn.String(),
	}
}

func HypertableSchema(hypertableSchemaName string, columns []systemcatalog.Column) schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameType: string(schemamodel.STRUCT),
		schemamodel.FieldNameFields: func() []schemamodel.Struct {
			fields := make([]schemamodel.Struct, len(columns))
			for i, column := range columns {
				fields[i] = column2field(column)
			}
			return fields
		}(),
		schemamodel.FieldNameName: hypertableSchemaName,
	}
}

func KeySchema(hypertable *systemcatalog.Hypertable, topicSchemaGenerator namegenerator.NameGenerator) schemamodel.Struct {
	schemaTopicName := topicSchemaGenerator.SchemaTopicName(hypertable)
	hypertableKeySchemaName := fmt.Sprintf("%s.Key", schemaTopicName)

	return schemamodel.Struct{
		schemamodel.FieldNameType:     string(schemamodel.STRUCT),
		schemamodel.FieldNameName:     hypertableKeySchemaName,
		schemamodel.FieldNameOptional: false,
		schemamodel.FieldNameFields: func() []schemamodel.Struct {
			keys := make([]schemamodel.Struct, 0)
			fieldIndex := 0
			for _, column := range hypertable.Columns() {
				if !column.IsPrimaryKey() {
					continue
				}
				keys = append(keys, keySchemaElement(column.Name(), fieldIndex, column.PgType().SchemaType(), false))
				fieldIndex++
			}
			return keys
		}(),
	}
}

func TimescaleEventKeySchema() schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameType:     string(schemamodel.STRUCT),
		schemamodel.FieldNameName:     TimescaleEventSchemaName,
		schemamodel.FieldNameOptional: false,
		schemamodel.FieldNameFields: []schemamodel.Struct{
			simpleSchemaElement(schemamodel.FieldNameSchema, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTable, schemamodel.STRING, false),
		},
	}
}

func EnvelopeSchema(schemaRegistry Registry, topicSchemaGenerator namegenerator.NameGenerator,
	hypertable *systemcatalog.Hypertable) schemamodel.Struct {

	schemaTopicName := topicSchemaGenerator.SchemaTopicName(hypertable)
	hypertableSchemaName := fmt.Sprintf("%s.Value", schemaTopicName)
	envelopeSchemaName := fmt.Sprintf("%s.Envelope", schemaTopicName)
	hypertableSchema := schemaRegistry.GetSchemaOrCreate(hypertableSchemaName, func() schemamodel.Struct {
		return HypertableSchema(hypertableSchemaName, hypertable.Columns())
	})

	return schemamodel.Struct{
		schemamodel.FieldNameType: string(schemamodel.STRUCT),
		schemamodel.FieldNameFields: []schemamodel.Struct{
			extendHypertableSchema(hypertableSchema, schemamodel.FieldNameBefore, true),
			extendHypertableSchema(hypertableSchema, schemamodel.FieldNameAfter, false),
			schemaRegistry.GetSchema(SourceSchemaName),
			simpleSchemaElement(schemamodel.FieldNameOperation, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTimescaleOp, schemamodel.STRING, true),
			simpleSchemaElement(schemamodel.FieldNameTimestamp, schemamodel.INT64, true),
		},
		schemamodel.FieldNameOptional: false,
		schemamodel.FieldNameName:     envelopeSchemaName,
	}
}

func EnvelopeMessageSchema(schemaRegistry Registry, topicSchemaGenerator namegenerator.NameGenerator) schemamodel.Struct {
	schemaTopicName := topicSchemaGenerator.MessageTopicName()
	envelopeSchemaName := fmt.Sprintf("%s.Envelope", schemaTopicName)

	return schemamodel.Struct{
		schemamodel.FieldNameType: string(schemamodel.STRUCT),
		schemamodel.FieldNameFields: []schemamodel.Struct{
			schemaRegistry.GetSchema(MessageValueSchemaName),
			schemaRegistry.GetSchema(SourceSchemaName),
			simpleSchemaElement(schemamodel.FieldNameOperation, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTimescaleOp, schemamodel.STRING, true),
			simpleSchemaElement(schemamodel.FieldNameTimestamp, schemamodel.INT64, true),
		},
		schemamodel.FieldNameOptional: false,
		schemamodel.FieldNameName:     envelopeSchemaName,
	}
}

func SourceSchema() schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameType: string(schemamodel.STRUCT),
		schemamodel.FieldNameFields: []schemamodel.Struct{
			simpleSchemaElement(schemamodel.FieldNameVersion, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameConnector, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameName, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTimestamp, schemamodel.STRING, false),
			simpleSchemaElementWithDefault(schemamodel.FieldNameSnapshot, schemamodel.BOOLEAN, true, false),
			simpleSchemaElement(schemamodel.FieldNameSchema, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTable, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTxId, schemamodel.INT64, true),
			simpleSchemaElement(schemamodel.FieldNameLSN, schemamodel.INT64, true),
			simpleSchemaElement(schemamodel.FieldNameXmin, schemamodel.INT64, true),
		},
		schemamodel.FieldNameOptional: false,
		schemamodel.FieldNameName:     SourceSchemaName,
		schemamodel.FieldNameField:    schemamodel.FieldNameSource,
	}
}

func MessageValueSchema(schemaRegistry Registry) schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameVersion: 1,
		schemamodel.FieldNameName:    MessageValueSchemaName,
		schemamodel.FieldNameFields: []schemamodel.Struct{
			simpleSchemaElement(schemamodel.FieldNameOperation, schemamodel.STRING, false),
			simpleSchemaElement(schemamodel.FieldNameTimestamp, schemamodel.INT64, true),
			schemaRegistry.GetSchema(SourceSchemaName),
			{
				schemamodel.FieldNameField:    schemamodel.FieldNameMessage,
				schemamodel.FieldNameOptional: false,
				schemamodel.FieldNameMessage:  messageBlockSchema(),
			},
		},
	}
}

func MessageKeySchema() schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameVersion: 1,
		schemamodel.FieldNameName:    MessageKeySchemaName,
		schemamodel.FieldNameFields: []schemamodel.Struct{
			simpleSchemaElement(schemamodel.FieldNamePrefix, schemamodel.STRING, true),
		},
	}
}

func messageBlockSchema() schemamodel.Struct {
	return schemamodel.Struct{
		schemamodel.FieldNameVersion: 1,
		schemamodel.FieldNameName:    MessageBlockSchemaName,
		schemamodel.FieldNameFields: []schemamodel.Struct{
			simpleSchemaElement(schemamodel.FieldNamePrefix, schemamodel.STRING, true),
			simpleSchemaElement(schemamodel.FieldNameContent, schemamodel.STRING, true),
		},
	}
}

func simpleSchemaElement(fieldName schemamodel.SchemaField,
	schemaType schemamodel.SchemaType, optional bool) schemamodel.Struct {

	return schemamodel.Struct{
		schemamodel.FieldNameType:     string(schemaType),
		schemamodel.FieldNameOptional: optional,
		schemamodel.FieldNameField:    fieldName,
	}
}

func keySchemaElement(fieldName schemamodel.SchemaField, index int,
	schemaType schemamodel.SchemaType, optional bool) schemamodel.Struct {

	return schemamodel.Struct{
		schemamodel.FieldNameName:  fieldName,
		schemamodel.FieldNameIndex: index,
		schemamodel.FieldNameSchema: schemamodel.Struct{
			schemamodel.FieldNameType:     string(schemaType),
			schemamodel.FieldNameOptional: optional,
		},
	}
}

func simpleSchemaElementWithDefault(fieldName schemamodel.SchemaField,
	schemaType schemamodel.SchemaType, optional bool, defaultValue any) schemamodel.Struct {

	return schemamodel.Struct{
		schemamodel.FieldNameType:     string(schemaType),
		schemamodel.FieldNameOptional: optional,
		schemamodel.FieldNameDefault:  defaultValue,
		schemamodel.FieldNameField:    fieldName,
	}
}

func extendHypertableSchema(hypertableSchema schemamodel.Struct,
	fieldName schemamodel.SchemaField, optional bool) schemamodel.Struct {

	return schemamodel.Struct{
		schemamodel.FieldNameType:     string(schemamodel.STRUCT),
		schemamodel.FieldNameFields:   hypertableSchema[schemamodel.FieldNameFields],
		schemamodel.FieldNameName:     hypertableSchema[schemamodel.FieldNameName],
		schemamodel.FieldNameOptional: optional,
		schemamodel.FieldNameField:    fieldName,
	}
}

func column2field(colum systemcatalog.Column) schemamodel.Struct {
	pgType := colum.PgType()
	schemaBuilder := pgType.SchemaBuilder()

	field := schemamodel.Struct{
		schemamodel.FieldNameType:     schemaBuilder.BaseSchemaType(),
		schemamodel.FieldNameOptional: colum.IsNullable(),
		schemamodel.FieldNameField:    colum.Name(),
	}

	if pgType.IsArray() {
		field[schemamodel.FieldNameFields] = schemaBuilder.Schema()
	} else if pgType.IsRecord() {
		//todo: not yet supported
		panic("not yet implemented")
	}

	if colum.DefaultValue() != nil {
		defaultValue := *colum.DefaultValue()
		if v, err := strconv.ParseBool(defaultValue); err == nil {
			field[schemamodel.FieldNameDefault] = v
		} else if v, err := strconv.ParseInt(defaultValue, 10, 64); err == nil {
			field[schemamodel.FieldNameDefault] = v
		} else if v, err := strconv.ParseFloat(defaultValue, 64); err == nil {
			field[schemamodel.FieldNameDefault] = v
		} else {
			field[schemamodel.FieldNameDefault] = defaultValue
		}
	}
	return field
}
