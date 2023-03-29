package schema

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"strconv"
	"time"
)

const SourceSchemaName = "io.debezium.connector.postgresql.Source"

type Operation string

const (
	OP_READ   Operation = "r"
	OP_CREATE Operation = "c"
	OP_UPDATE  Operation = "u"
	OP_DELETE   Operation = "d"
	OP_TRUNCATE Operation = "t"
	OP_MESSAGE   Operation = "m"
	OP_TIMESCALE Operation = "$"
)

type TimescaleOperation string

const (
	OP_COMPRESSION   TimescaleOperation = "c"
	OP_DECOMPRESSION TimescaleOperation = "d"
)

const (
	fieldNameBefore      = "before"
	fieldNameAfter       = "after"
	fieldNameOperation   = "op"
	fieldNameSource      = "source"
	fieldNameTransaction = "transaction"
	fieldNameTimestamp   = "ts_ms"
	fieldNameTimescaleOp = "tsdb_op"
	fieldNameVersion     = "version"
	fieldNameSchema      = "schema"
	fieldNamePayload     = "payload"
	fieldNameConnector   = "connector"
	fieldNameName        = "name"
	fieldNameSnapshot    = "snapshot"
	fieldNameDatabase    = "db"
	fieldNameSequence    = "sequence"
	fieldNameTable       = "table"
	fieldNameTxId        = "txId"
	fieldNameLSN         = "lsn"
	fieldNameXmin        = "xmin"
	fieldNameType        = "type"
	fieldNameOptional    = "optional"
	fieldNameField       = "field"
	fieldNameFields      = "fields"
	fieldNameDefault     = "default"
)

type Struct = map[string]any

func ReadEvent(record Struct, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_READ
	event[fieldNameAfter] = record
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CreateEvent(record Struct, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_CREATE
	event[fieldNameAfter] = record
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func UpdateEvent(before, after, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_UPDATE
	if before != nil {
		event[fieldNameBefore] = before
	}
	event[fieldNameAfter] = after
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DeleteEvent(before, source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_DELETE
	event[fieldNameBefore] = before
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func TruncateEvent(source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_TRUNCATE
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func CompressionEvent(source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_TIMESCALE
	event[fieldNameTimescaleOp] = OP_COMPRESSION
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func DecompressionEvent(source Struct) Struct {
	event := make(Struct, 0)
	event[fieldNameOperation] = OP_TIMESCALE
	event[fieldNameTimescaleOp] = OP_DECOMPRESSION
	if source != nil {
		event[fieldNameSource] = source
	}
	event[fieldNameTimestamp] = time.Now().UnixMilli()
	return event
}

func Envelope(schema, payload Struct) Struct {
	return Struct{
		fieldNameSchema:  schema,
		fieldNamePayload: payload,
	}
}

func Source(lsn pglogrepl.LSN, timestamp time.Time, snapshot bool, hypertable *model.Hypertable) Struct {
	return Struct{
		fieldNameVersion:   "0.0.1", // FIXME, get a real version
		fieldNameConnector: "event-stream-prototype",
		fieldNameName:      hypertable.DatabaseName(),
		fieldNameTimestamp: timestamp.UnixMilli(),
		fieldNameSnapshot:  snapshot,
		fieldNameDatabase:  hypertable.DatabaseName(),
		fieldNameSchema:    hypertable.SchemaName(),
		fieldNameTable:     hypertable.HypertableName(),
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

func simpleSchemaElement(fieldName, typeName model.DataType, optional bool) Struct {
	return Struct{
		fieldNameType:     string(typeName),
		fieldNameOptional: optional,
		fieldNameField:    fieldName,
	}
}

func simpleSchemaElementWithDefault(fieldName, typeName model.DataType, optional bool, defaultValue any) Struct {
	return Struct{
		fieldNameType:     string(typeName),
		fieldNameOptional: optional,
		fieldNameDefault:  defaultValue,
		fieldNameField:    fieldName,
	}
}

func extendHypertableSchema(hypertableSchema Struct, fieldName string, optional bool) Struct {
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
		fieldNameOptional: colum.Nullable(),
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
