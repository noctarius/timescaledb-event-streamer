package schema

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/event/topic"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog/model"
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

type schemaField = string

const (
	fieldNameBefore      schemaField = "before"
	fieldNameAfter       schemaField = "after"
	fieldNameOperation   schemaField = "op"
	fieldNameSource      schemaField = "source"
	fieldNameTransaction schemaField = "transaction"
	fieldNameTimestamp   schemaField = "ts_ms"
	fieldNameTimescaleOp schemaField = "tsdb_op"
	fieldNameVersion     schemaField = "version"
	fieldNameSchema      schemaField = "schema"
	fieldNamePayload     schemaField = "payload"
	fieldNameConnector   schemaField = "connector"
	fieldNameName        schemaField = "name"
	fieldNameSnapshot    schemaField = "snapshot"
	fieldNameDatabase    schemaField = "db"
	fieldNameSequence    schemaField = "sequence"
	fieldNameTable       schemaField = "table"
	fieldNameTxId        schemaField = "txId"
	fieldNameLSN         schemaField = "lsn"
	fieldNameXmin        schemaField = "xmin"
	fieldNameType        schemaField = "type"
	fieldNameOptional    schemaField = "optional"
	fieldNameField       schemaField = "field"
	fieldNameFields      schemaField = "fields"
	fieldNameDefault     schemaField = "default"
	fieldNamePrefix      schemaField = "prefix"
	fieldNameContent     schemaField = "content"
	fieldNameMessage     schemaField = "message"
	fieldNameIndex       schemaField = "index"
)

type Struct = map[schemaField]any

func ReadEvent(record Struct, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_READ)
	event[fieldNameAfter] = record
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CreateEvent(record Struct, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_CREATE)
	event[fieldNameAfter] = record
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func UpdateEvent(before, after, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_UPDATE)
	if before != nil {
		event[fieldNameBefore] = before
	}
	if after != nil {
		event[fieldNameAfter] = after
	}
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DeleteEvent(before, source Struct, tombstone bool) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_DELETE)
	if before != nil {
		event[fieldNameBefore] = before
	}
	if tombstone {
		event[fieldNameAfter] = nil
	}
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func TruncateEvent(source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_TRUNCATE)
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func MessageEvent(prefix, content string, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_MESSAGE)
	event[fieldNameMessage] = Struct{
		fieldNamePrefix:  prefix,
		fieldNameContent: content,
	}
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CompressionEvent(source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_TIMESCALE)
	event[fieldNameTimescaleOp] = string(OP_COMPRESSION)
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DecompressionEvent(source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = string(OP_TIMESCALE)
	event[fieldNameTimescaleOp] = string(OP_DECOMPRESSION)
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func MessageKey(prefix string) Struct {
	return Struct{
		fieldNamePrefix: prefix,
	}
}

func TimescaleKey(schemaName, tableName string) Struct {
	return Struct{
		fieldNameSchema: schemaName,
		fieldNameTable:  tableName,
	}
}

func Envelope(schema, payload Struct) Struct {
	return Struct{
		fieldNameSchema:  schema,
		fieldNamePayload: payload,
	}
}

func Source(lsn pglogrepl.LSN, timestamp time.Time, snapshot bool,
	databaseName, schemaName, hypertableName string, transactionId *uint32) Struct {

	return Struct{
		fieldNameVersion:   "0.0.1", // FIXME, get a real version
		fieldNameConnector: "event-stream-prototype",
		fieldNameName:      databaseName,
		fieldNameTimestamp: timestamp.UnixMilli(),
		fieldNameSnapshot:  snapshot,
		fieldNameDatabase:  databaseName,
		fieldNameSchema:    schemaName,
		fieldNameTable:     hypertableName,
		fieldNameTxId:      transactionId,
		fieldNameLSN:       lsn.String(),
	}
}

func HypertableSchema(hypertableSchemaName string, columns []model.Column) Struct {
	return Struct{
		fieldNameType: string(model.STRUCT),
		fieldNameFields: func() []Struct {
			fields := make([]Struct, len(columns))
			for i, column := range columns {
				fields[i] = column2field(column)
			}
			return fields
		}(),
		fieldNameName: hypertableSchemaName,
	}
}

func KeySchema(hypertable *model.Hypertable, topicSchemaGenerator *topic.NameGenerator) Struct {
	schemaTopicName := topicSchemaGenerator.SchemaTopicName(hypertable)
	hypertableKeySchemaName := fmt.Sprintf("%s.Key", schemaTopicName)

	return Struct{
		fieldNameType:     string(model.STRUCT),
		fieldNameName:     hypertableKeySchemaName,
		fieldNameOptional: false,
		fieldNameFields: func() []Struct {
			keys := make([]Struct, 0)
			fieldIndex := 0
			for _, column := range hypertable.Columns() {
				if !column.IsPrimaryKey() {
					continue
				}
				keys = append(keys, keySchemaElement(column.Name(), fieldIndex, column.TypeName(), false))
				fieldIndex++
			}
			return keys
		}(),
	}
}

func TimescaleEventKeySchema() Struct {
	return Struct{
		fieldNameType:     string(model.STRUCT),
		fieldNameName:     TimescaleEventSchemaName,
		fieldNameOptional: false,
		fieldNameFields: []Struct{
			simpleSchemaElement(fieldNameSchema, model.STRING, false),
			simpleSchemaElement(fieldNameTable, model.STRING, false),
		},
	}
}

func EnvelopeSchema(schemaRegistry *Registry, hypertable *model.Hypertable,
	topicSchemaGenerator *topic.NameGenerator) Struct {

	schemaTopicName := topicSchemaGenerator.SchemaTopicName(hypertable)
	hypertableSchemaName := fmt.Sprintf("%s.Value", schemaTopicName)
	envelopeSchemaName := fmt.Sprintf("%s.Envelope", schemaTopicName)
	hypertableSchema := schemaRegistry.GetSchemaOrCreate(hypertableSchemaName, func() Struct {
		return HypertableSchema(hypertableSchemaName, hypertable.Columns())
	})

	return Struct{
		fieldNameType: string(model.STRUCT),
		fieldNameFields: []Struct{
			extendHypertableSchema(hypertableSchema, fieldNameBefore, true),
			extendHypertableSchema(hypertableSchema, fieldNameAfter, false),
			schemaRegistry.GetSchema(SourceSchemaName),
			simpleSchemaElement(fieldNameOperation, model.STRING, false),
			simpleSchemaElement(fieldNameTimescaleOp, model.STRING, true),
			simpleSchemaElement(fieldNameTimestamp, model.INT64, true),
		},
		fieldNameOptional: false,
		fieldNameName:     envelopeSchemaName,
	}
}

func EnvelopeMessageSchema(schemaRegistry *Registry,
	topicSchemaGenerator *topic.NameGenerator) Struct {

	schemaTopicName := topicSchemaGenerator.MessageTopicName()
	envelopeSchemaName := fmt.Sprintf("%s.Envelope", schemaTopicName)

	return Struct{
		fieldNameType: string(model.STRUCT),
		fieldNameFields: []Struct{
			schemaRegistry.GetSchema(MessageValueSchemaName),
			schemaRegistry.GetSchema(SourceSchemaName),
			simpleSchemaElement(fieldNameOperation, model.STRING, false),
			simpleSchemaElement(fieldNameTimescaleOp, model.STRING, true),
			simpleSchemaElement(fieldNameTimestamp, model.INT64, true),
		},
		fieldNameOptional: false,
		fieldNameName:     envelopeSchemaName,
	}
}

func sourceSchema() Struct {
	return Struct{
		fieldNameType: string(model.STRUCT),
		fieldNameFields: []Struct{
			simpleSchemaElement(fieldNameVersion, model.STRING, false),
			simpleSchemaElement(fieldNameConnector, model.STRING, false),
			simpleSchemaElement(fieldNameName, model.STRING, false),
			simpleSchemaElement(fieldNameTimestamp, model.STRING, false),
			simpleSchemaElementWithDefault(fieldNameSnapshot, model.BOOLEAN, true, false),
			simpleSchemaElement(fieldNameSchema, model.STRING, false),
			simpleSchemaElement(fieldNameTable, model.STRING, false),
			simpleSchemaElement(fieldNameTxId, model.INT64, true),
			simpleSchemaElement(fieldNameLSN, model.INT64, true),
			simpleSchemaElement(fieldNameXmin, model.INT64, true),
		},
		fieldNameOptional: false,
		fieldNameName:     SourceSchemaName,
		fieldNameField:    fieldNameSource,
	}
}

func messageValueSchema(schemaRegistry *Registry) Struct {
	return Struct{
		fieldNameVersion: 1,
		fieldNameName:    MessageValueSchemaName,
		fieldNameFields: []Struct{
			simpleSchemaElement(fieldNameOperation, model.STRING, false),
			simpleSchemaElement(fieldNameTimestamp, model.INT64, true),
			schemaRegistry.GetSchema(SourceSchemaName),
			{
				fieldNameField:    fieldNameMessage,
				fieldNameOptional: false,
				fieldNameMessage:  messageBlockSchema(),
			},
		},
	}
}

func messageKeySchema() Struct {
	return Struct{
		fieldNameVersion: 1,
		fieldNameName:    MessageKeySchemaName,
		fieldNameFields: []Struct{
			simpleSchemaElement(fieldNamePrefix, model.STRING, true),
		},
	}
}

func messageBlockSchema() Struct {
	return Struct{
		fieldNameVersion: 1,
		fieldNameName:    MessageBlockSchemaName,
		fieldNameFields: []Struct{
			simpleSchemaElement(fieldNamePrefix, model.STRING, true),
			simpleSchemaElement(fieldNameContent, model.STRING, true),
		},
	}
}

func simpleSchemaElement(fieldName schemaField, typeName model.DataType, optional bool) Struct {
	return Struct{
		fieldNameType:     string(typeName),
		fieldNameOptional: optional,
		fieldNameField:    fieldName,
	}
}

func keySchemaElement(fieldName schemaField, index int, typeName string, optional bool) Struct {
	return Struct{
		fieldNameName:  fieldName,
		fieldNameIndex: index,
		fieldNameSchema: Struct{
			fieldNameType:     typeName,
			fieldNameOptional: optional,
		},
	}
}

func simpleSchemaElementWithDefault(fieldName schemaField,
	typeName model.DataType, optional bool, defaultValue any) Struct {

	return Struct{
		fieldNameType:     string(typeName),
		fieldNameOptional: optional,
		fieldNameDefault:  defaultValue,
		fieldNameField:    fieldName,
	}
}

func extendHypertableSchema(hypertableSchema Struct, fieldName schemaField, optional bool) Struct {
	return Struct{
		fieldNameType:     string(model.STRUCT),
		fieldNameFields:   hypertableSchema[fieldNameFields],
		fieldNameName:     hypertableSchema[fieldNameName],
		fieldNameOptional: optional,
		fieldNameField:    fieldName,
	}
}

func column2field(colum model.Column) Struct {
	field := Struct{
		fieldNameType:     colum.TypeName(),
		fieldNameOptional: colum.IsNullable(),
		fieldNameField:    colum.Name(),
	}
	if colum.DefaultValue() != nil {
		defaultValue := *colum.DefaultValue()
		if v, err := strconv.ParseBool(defaultValue); err == nil {
			field[fieldNameDefault] = v
		} else if v, err := strconv.ParseInt(defaultValue, 10, 64); err == nil {
			field[fieldNameDefault] = v
		} else if v, err := strconv.ParseFloat(defaultValue, 64); err == nil {
			field[fieldNameDefault] = v
		} else {
			field[fieldNameDefault] = defaultValue
		}
	}
	return field
}
