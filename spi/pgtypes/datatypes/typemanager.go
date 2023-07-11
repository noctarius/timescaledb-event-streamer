package datatypes

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"sync"
)

type TypeFactory func(name string, typ TypeType, oid uint32, category TypeCategory, arrayType bool, oidArray uint32,
	oidElement uint32, recordType bool, parentOid uint32, modifiers int, enumValues []string, delimiter string) Type

type TypeResolver interface {
	ReadPgTypes(factory TypeFactory, callback func(Type) error) error
	ReadPgType(oid uint32, factory TypeFactory) (Type, bool, error)
}

var coreTypes = map[uint32]schemamodel.SchemaType{
	pgtype.BoolOID:        schemamodel.BOOLEAN,
	pgtype.Int2OID:        schemamodel.INT16,
	pgtype.Int4OID:        schemamodel.INT32,
	pgtype.Int8OID:        schemamodel.INT64,
	pgtype.Float4OID:      schemamodel.FLOAT32,
	pgtype.Float8OID:      schemamodel.FLOAT64,
	pgtype.BPCharOID:      schemamodel.STRING,
	pgtype.QCharOID:       schemamodel.STRING,
	pgtype.VarcharOID:     schemamodel.STRING,
	pgtype.TextOID:        schemamodel.STRING,
	pgtype.TimestampOID:   schemamodel.INT64,
	pgtype.TimestamptzOID: schemamodel.STRING,
	pgtype.IntervalOID:    schemamodel.INT64,
	pgtype.ByteaOID:       schemamodel.BYTES,
	pgtype.JSONOID:        schemamodel.STRING,
	pgtype.JSONBOID:       schemamodel.STRING,
	pgtype.UUIDOID:        schemamodel.STRING,
	pgtype.NameOID:        schemamodel.STRING,
	pgtype.OIDOID:         schemamodel.INT64,
	pgtype.TIDOID:         schemamodel.INT64,
	pgtype.XIDOID:         schemamodel.INT64,
	pgtype.CIDOID:         schemamodel.INT64,
	pgtype.CIDROID:        schemamodel.STRING,
	pgtype.MacaddrOID:     schemamodel.STRING,
	774:                   schemamodel.STRING, //macaddr8
	pgtype.InetOID:        schemamodel.STRING,
	pgtype.DateOID:        schemamodel.STRING,
	pgtype.TimeOID:        schemamodel.STRING,
	pgtype.NumericOID:     schemamodel.STRUCT,
}

var converters = map[uint32]Converter{
	pgtype.BoolOID:        nil,
	pgtype.Int2OID:        nil,
	pgtype.Int4OID:        nil,
	pgtype.Int8OID:        nil,
	pgtype.Float4OID:      nil,
	pgtype.Float8OID:      nil,
	pgtype.BPCharOID:      nil,
	pgtype.QCharOID:       char2text,
	pgtype.VarcharOID:     nil,
	pgtype.TextOID:        nil,
	pgtype.TimestampOID:   timestamp2int64,
	pgtype.TimestamptzOID: timestamp2text,
	pgtype.IntervalOID:    interval2int64,
	pgtype.ByteaOID:       nil,
	pgtype.JSONOID:        json2text,
	pgtype.JSONBOID:       json2text,
	pgtype.UUIDOID:        uuid2text,
	pgtype.NameOID:        nil,
	pgtype.OIDOID:         uint322int64,
	pgtype.TIDOID:         uint322int64,
	pgtype.XIDOID:         uint322int64,
	pgtype.CIDOID:         uint322int64,
	pgtype.CIDROID:        addr2text,
	pgtype.MacaddrOID:     macaddr2text,
	774:                   macaddr2text, // macaddr8
	pgtype.InetOID:        addr2text,
	pgtype.DateOID:        timestamp2text,
	pgtype.TimeOID:        time2text,
	pgtype.NumericOID:     numeric2variableScaleDecimal,
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
	typeCache           map[uint32]Type
	typeCacheMutex      sync.Mutex
	optimizedTypes      map[uint32]Type
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
		typeCache:           make(map[uint32]Type),
		typeCacheMutex:      sync.Mutex{},
		optimizedTypes:      make(map[uint32]Type),
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

	if err := tm.typeResolver.ReadPgTypes(tm.typeFactory, func(typ Type) error {
		tm.typeCache[typ.Oid()] = typ

		if supporting.IndexOf(optimizedTypes, typ.name) != -1 {
			tm.optimizedTypes[typ.oid] = typ
			tm.optimizedConverters[typ.oid] = nil // FIXME
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (tm *TypeManager) typeFactory(name string, typ TypeType, oid uint32, category TypeCategory,
	arrayType bool, oidArray uint32, oidElement uint32, recordType bool, parentOid uint32, modifiers int,
	enumValues []string, delimiter string) Type {

	pgType := NewType(name, typ, oid, category, arrayType, oidArray, oidElement,
		recordType, parentOid, modifiers, enumValues, delimiter)

	pgType.typeManager = tm
	pgType.schemaBuilder = resolveSchemaBuilder(pgType)

	return pgType
}

func (tm *TypeManager) DataType(oid uint32) (Type, error) {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()

	// Is it already available / cached?
	dataType, present := tm.typeCache[oid]
	if present {
		return dataType, nil
	}

	dataType, found, err := tm.typeResolver.ReadPgType(oid, tm.typeFactory)
	if err != nil {
		return Type{}, err
	}

	if !found {
		return Type{}, errors.Errorf("illegal oid: %d", oid)
	}

	tm.typeCache[oid] = dataType
	return dataType, nil
}

func (tm *TypeManager) SchemaBuilder(oid uint32) schemamodel.SchemaBuilder {

	//TODO implement me
	panic("implement me")
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

func getSchemaType(oid uint32, arrayType bool, typType TypeType) schemamodel.SchemaType {
	if coreType, present := coreTypes[oid]; present {
		return coreType
	}
	if arrayType {
		return schemamodel.ARRAY
	} else if typType == EnumType {
		return schemamodel.STRING
	}
	return schemamodel.STRUCT
}

func resolveSchemaBuilder(pgType Type) schemamodel.SchemaBuilder {
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
	pgType        Type
	schemaBuilder schemamodel.SchemaBuilder
}

func (l *lazySchemaBuilder) BaseSchemaType() schemamodel.SchemaType {
	return l.pgType.schemaType
}

func (l *lazySchemaBuilder) Schema(oid uint32, modifier int) schemamodel.Struct {
	if l.schemaBuilder == nil {
		l.schemaBuilder = l.pgType.resolveSchemaBuilder(oid, modifier)
	}
	return l.schemaBuilder.Schema(oid, modifier)
}
