package datatypes

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"sync"
)

type TypeFactory func(name string, kind systemcatalog.PgKind, oid uint32, category systemcatalog.PgCategory,
	arrayType bool, recordType bool, oidArray uint32, oidElement uint32, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) systemcatalog.PgType

type TypeResolver interface {
	ReadPgTypes(factory TypeFactory, callback func(systemcatalog.PgType) error) error
	ReadPgType(oid uint32, factory TypeFactory) (systemcatalog.PgType, bool, error)
}

var coreTypes = map[uint32]schemamodel.SchemaType{
	pgtype.BoolOID:             schemamodel.BOOLEAN,
	pgtype.BoolArrayOID:        schemamodel.ARRAY,
	pgtype.Int2OID:             schemamodel.INT16,
	pgtype.Int2ArrayOID:        schemamodel.ARRAY,
	pgtype.Int4OID:             schemamodel.INT32,
	pgtype.Int4ArrayOID:        schemamodel.ARRAY,
	pgtype.Int8OID:             schemamodel.INT64,
	pgtype.Int8ArrayOID:        schemamodel.ARRAY,
	pgtype.Float4OID:           schemamodel.FLOAT32,
	pgtype.Float4ArrayOID:      schemamodel.ARRAY,
	pgtype.Float8OID:           schemamodel.FLOAT64,
	pgtype.Float8ArrayOID:      schemamodel.ARRAY,
	pgtype.BPCharOID:           schemamodel.STRING,
	pgtype.QCharOID:            schemamodel.STRING,
	pgtype.VarcharOID:          schemamodel.STRING,
	pgtype.VarcharArrayOID:     schemamodel.ARRAY,
	pgtype.TextOID:             schemamodel.STRING,
	pgtype.TextArrayOID:        schemamodel.ARRAY,
	pgtype.TimestampOID:        schemamodel.INT64,
	pgtype.TimestampArrayOID:   schemamodel.ARRAY,
	pgtype.TimestamptzOID:      schemamodel.STRING,
	pgtype.TimestamptzArrayOID: schemamodel.ARRAY,
	pgtype.IntervalOID:         schemamodel.INT64,
	pgtype.IntervalArrayOID:    schemamodel.ARRAY,
	pgtype.ByteaOID:            schemamodel.BYTES,
	pgtype.ByteaArrayOID:       schemamodel.ARRAY,
	pgtype.JSONOID:             schemamodel.STRING,
	pgtype.JSONArrayOID:        schemamodel.ARRAY,
	pgtype.JSONBOID:            schemamodel.STRING,
	pgtype.JSONBArrayOID:       schemamodel.ARRAY,
	pgtype.UUIDOID:             schemamodel.STRING,
	pgtype.UUIDArrayOID:        schemamodel.ARRAY,
	pgtype.NameOID:             schemamodel.STRING,
	pgtype.NameArrayOID:        schemamodel.ARRAY,
	pgtype.OIDOID:              schemamodel.INT64,
	pgtype.TIDOID:              schemamodel.INT64,
	pgtype.XIDOID:              schemamodel.INT64,
	pgtype.CIDOID:              schemamodel.INT64,
	pgtype.CIDROID:             schemamodel.STRING,
	pgtype.MacaddrOID:          schemamodel.STRING,
	pgtype.MacaddrArrayOID:     schemamodel.ARRAY,
	774:                        schemamodel.STRING, // macaddr8
	775:                        schemamodel.ARRAY,  // macaddr8[]
	pgtype.InetOID:             schemamodel.STRING,
	pgtype.InetArrayOID:        schemamodel.ARRAY,
	pgtype.DateOID:             schemamodel.STRING,
	pgtype.DateArrayOID:        schemamodel.ARRAY,
	pgtype.TimeOID:             schemamodel.STRING,
	pgtype.TimeArrayOID:        schemamodel.ARRAY,
	pgtype.NumericOID:          schemamodel.STRUCT,
}

var converters = map[uint32]Converter{
	pgtype.BoolOID:             nil,
	pgtype.BoolArrayOID:        nil,
	pgtype.Int2OID:             nil,
	pgtype.Int2ArrayOID:        nil,
	pgtype.Int4OID:             nil,
	pgtype.Int4ArrayOID:        nil,
	pgtype.Int8OID:             nil,
	pgtype.Int8ArrayOID:        nil,
	pgtype.Float4OID:           nil,
	pgtype.Float4ArrayOID:      nil,
	pgtype.Float8OID:           nil,
	pgtype.Float8ArrayOID:      nil,
	pgtype.BPCharOID:           nil,
	pgtype.QCharOID:            char2text,
	pgtype.VarcharOID:          nil,
	pgtype.VarcharArrayOID:     nil,
	pgtype.TextOID:             nil,
	pgtype.TextArrayOID:        nil,
	pgtype.TimestampOID:        timestamp2int64,
	pgtype.TimestampArrayOID:   arrayConverter[[]int64](pgtype.TimestampOID, timestamp2int64),
	pgtype.TimestamptzOID:      timestamp2text,
	pgtype.TimestamptzArrayOID: arrayConverter[[]string](pgtype.TimestamptzOID, timestamp2text),
	pgtype.IntervalOID:         interval2int64,
	pgtype.IntervalArrayOID:    arrayConverter[[]int64](pgtype.IntervalOID, interval2int64),
	pgtype.ByteaOID:            nil,
	pgtype.ByteaArrayOID:       arrayConverter[[][]byte](pgtype.ByteaOID, nil),
	pgtype.JSONOID:             json2text,
	pgtype.JSONArrayOID:        arrayConverter[[]string](pgtype.JSONOID, json2text),
	pgtype.JSONBOID:            json2text,
	pgtype.JSONBArrayOID:       arrayConverter[[]string](pgtype.JSONBOID, json2text),
	pgtype.UUIDOID:             uuid2text,
	pgtype.UUIDArrayOID:        arrayConverter[[]string](pgtype.UUIDOID, uuid2text),
	pgtype.NameOID:             nil,
	pgtype.NameArrayOID:        nil,
	pgtype.OIDOID:              uint322int64,
	pgtype.TIDOID:              uint322int64,
	pgtype.XIDOID:              uint322int64,
	pgtype.CIDOID:              uint322int64,
	pgtype.CIDROID:             addr2text,
	pgtype.CIDRArrayOID:        arrayConverter[[]string](pgtype.CIDROID, addr2text),
	pgtype.MacaddrOID:          macaddr2text,
	pgtype.MacaddrArrayOID:     arrayConverter[[]string](pgtype.MacaddrOID, macaddr2text),
	774:                        macaddr2text,                                // macaddr8
	775:                        arrayConverter[[]string](774, macaddr2text), // macaddr8[]
	pgtype.InetOID:             addr2text,
	pgtype.InetArrayOID:        arrayConverter[[]string](pgtype.InetOID, addr2text),
	pgtype.DateOID:             timestamp2text,
	pgtype.DateArrayOID:        arrayConverter[[]string](pgtype.DateOID, timestamp2text),
	pgtype.TimeOID:             time2text,
	pgtype.TimeArrayOID:        arrayConverter[[]string](pgtype.TimeOID, time2text),
	pgtype.NumericOID:          numeric2variableScaleDecimal,
	//1002:                   arrayConverter[[]string](pgtype.QCharOID, char2text), // QCharArrayOID
}

var optimizedTypes = []string{
	"geometry", "ltree",
}

// ErrIllegalValue represents an illegal type conversion request
// for the given value
var ErrIllegalValue = fmt.Errorf("illegal value for data type conversion")

type TypeManager struct {
	logger              *logging.Logger
	typeResolver        TypeResolver
	typeCache           map[uint32]systemcatalog.PgType
	typeCacheMutex      sync.Mutex
	optimizedTypes      map[uint32]systemcatalog.PgType
	optimizedConverters map[uint32]Converter
}

func NewTypeManager(typeResolver TypeResolver) (*TypeManager, error) {
	logger, err := logging.NewLogger("TypeManager")
	if err != nil {
		return nil, err
	}

	typeManager := &TypeManager{
		logger:              logger,
		typeResolver:        typeResolver,
		typeCache:           make(map[uint32]systemcatalog.PgType),
		typeCacheMutex:      sync.Mutex{},
		optimizedTypes:      make(map[uint32]systemcatalog.PgType),
		optimizedConverters: make(map[uint32]Converter),
	}

	if err := typeManager.initialize(); err != nil {
		return nil, err
	}
	return typeManager, nil
}

func (tm *TypeManager) initialize() error {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()

	coreTypesSlice := supporting.MapMapper(coreTypes, func(key uint32, _ schemamodel.SchemaType) uint32 {
		return key
	})

	if err := tm.typeResolver.ReadPgTypes(tm.typeFactory, func(typ systemcatalog.PgType) error {
		if supporting.IndexOf(coreTypesSlice, typ.Oid()) != -1 {
			return nil
		}

		tm.typeCache[typ.Oid()] = typ

		if supporting.IndexOf(optimizedTypes, typ.Name()) != -1 {
			tm.optimizedTypes[typ.Oid()] = typ
			tm.optimizedConverters[typ.Oid()] = nil // FIXME
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (tm *TypeManager) typeFactory(name string, kind systemcatalog.PgKind, oid uint32,
	category systemcatalog.PgCategory, arrayType bool, recordType bool, oidArray uint32, oidElement uint32,
	oidParent uint32, modifiers int, enumValues []string, delimiter string) systemcatalog.PgType {

	pgType := newType(name, kind, oid, category, arrayType, recordType, oidArray,
		oidElement, oidParent, modifiers, enumValues, delimiter)

	pgType.typeManager = tm
	pgType.schemaBuilder = resolveSchemaBuilder(pgType)

	return pgType
}

func (tm *TypeManager) DataType(oid uint32) (systemcatalog.PgType, error) {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()

	// Is it already available / cached?
	dataType, present := tm.typeCache[oid]
	if present {
		return dataType, nil
	}

	dataType, found, err := tm.typeResolver.ReadPgType(oid, tm.typeFactory)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, errors.Errorf("illegal oid: %d", oid)
	}

	tm.typeCache[oid] = dataType
	return dataType, nil
}

func (tm *TypeManager) Converter(oid uint32) (Converter, error) {
	if converter, present := converters[oid]; present {
		return converter, nil
	}
	if converter, present := tm.optimizedConverters[oid]; present {
		return converter, nil
	}
	return nil, fmt.Errorf("unsupported OID: %d", oid)
}

func (tm *TypeManager) NumKnownTypes() int {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()
	return len(tm.typeCache)
}

func getSchemaType(oid uint32, arrayType bool, kind systemcatalog.PgKind) schemamodel.SchemaType {
	if coreType, present := coreTypes[oid]; present {
		return coreType
	}
	if arrayType {
		return schemamodel.ARRAY
	} else if kind == systemcatalog.EnumKind {
		return schemamodel.STRING
	}
	return schemamodel.STRUCT
}

func resolveSchemaBuilder(pgType *pgType) schemamodel.SchemaBuilder {
	switch pgType.schemaType {
	case schemamodel.INT8:
		return schemamodel.Int8()
	case schemamodel.INT16:
		return schemamodel.Int16()
	case schemamodel.INT32:
		return schemamodel.Int32()
	case schemamodel.INT64:
		return schemamodel.Int64()
	case schemamodel.FLOAT32:
		return schemamodel.Float32()
	case schemamodel.FLOAT64:
		return schemamodel.Float64()
	case schemamodel.BOOLEAN:
		return schemamodel.Boolean()
	case schemamodel.STRING:
		return schemamodel.String()
	default:
		return &lazySchemaBuilder{pgType: pgType}
	}
}

type lazySchemaBuilder struct {
	pgType        *pgType
	schemaBuilder schemamodel.SchemaBuilder
}

func (l *lazySchemaBuilder) BaseSchemaType() schemamodel.SchemaType {
	return l.pgType.schemaType
}

func (l *lazySchemaBuilder) Schema(column schemamodel.ColumnDescriptor) schemamodel.Struct {
	if l.schemaBuilder == nil {
		l.schemaBuilder = l.pgType.resolveSchemaBuilder()
	}
	return l.schemaBuilder.Schema(column)
}
