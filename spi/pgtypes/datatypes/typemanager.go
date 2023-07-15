package datatypes

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/pgdecoding"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"reflect"
	"sync"
)

var (
	int8Type    = reflect.TypeOf(int8(0))
	int16Type   = reflect.TypeOf(int16(0))
	int32Type   = reflect.TypeOf(int32(0))
	int64Type   = reflect.TypeOf(int64(0))
	float32Type = reflect.TypeOf(float32(0))
	float64Type = reflect.TypeOf(float64(0))
	booleanType = reflect.TypeOf(true)
	stringType  = reflect.TypeOf("")
	byteaType   = reflect.TypeOf([]byte{})
	mapType     = reflect.TypeOf(map[string]any{})
)

type TypeFactory func(name string, kind systemcatalog.PgKind, oid uint32, category systemcatalog.PgCategory,
	arrayType bool, recordType bool, oidArray uint32, oidElement uint32, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) systemcatalog.PgType

type TypeResolver interface {
	ReadPgTypes(factory TypeFactory, callback func(systemcatalog.PgType) error) error
	ReadPgType(oid uint32, factory TypeFactory) (systemcatalog.PgType, bool, error)
}

type typeRegistration struct {
	schemaType schemamodel.SchemaType
	isArray    bool
	oidElement uint32
	converter  Converter
	codec      pgtype.Codec
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
	pgtype.IntervalOID:         schemamodel.INT64, // could also be a string: P1Y2M3DT4H5M6.78S
	pgtype.IntervalArrayOID:    schemamodel.ARRAY,
	pgtype.ByteaOID:            schemamodel.STRING,
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
	pgtype.OIDArrayOID:         schemamodel.ARRAY,
	pgtype.XIDOID:              schemamodel.INT64,
	pgtype.XIDArrayOID:         schemamodel.ARRAY,
	pgtype.CIDOID:              schemamodel.INT64,
	pgtype.CIDArrayOID:         schemamodel.ARRAY,
	pgtype.CIDROID:             schemamodel.STRING,
	pgtype.MacaddrOID:          schemamodel.STRING,
	pgtype.MacaddrArrayOID:     schemamodel.ARRAY,
	774:                        schemamodel.STRING, // macaddr8
	775:                        schemamodel.ARRAY,  // macaddr8[]
	pgtype.InetOID:             schemamodel.STRING,
	pgtype.InetArrayOID:        schemamodel.ARRAY,
	pgtype.DateOID:             schemamodel.INT32,
	pgtype.DateArrayOID:        schemamodel.ARRAY,
	pgtype.TimeOID:             schemamodel.STRING,
	pgtype.TimeArrayOID:        schemamodel.ARRAY,
	pgtype.NumericOID:          schemamodel.FLOAT64,
	pgtype.NumericArrayOID:     schemamodel.ARRAY,
	pgtype.Int4rangeOID:        schemamodel.STRING,
	pgtype.Int4rangeArrayOID:   schemamodel.ARRAY,
	pgtype.Int8rangeOID:        schemamodel.STRING,
	pgtype.Int8rangeArrayOID:   schemamodel.ARRAY,
	pgtype.NumrangeOID:         schemamodel.STRING,
	pgtype.NumrangeArrayOID:    schemamodel.ARRAY,
	pgtype.TsrangeOID:          schemamodel.STRING,
	pgtype.TsrangeArrayOID:     schemamodel.ARRAY,
	pgtype.TstzrangeOID:        schemamodel.STRING,
	pgtype.TstzrangeArrayOID:   schemamodel.ARRAY,
	pgtype.DaterangeOID:        schemamodel.STRING,
	pgtype.DaterangeArrayOID:   schemamodel.ARRAY,
	pgtype.BitOID:              schemamodel.STRING,
	pgtype.BitArrayOID:         schemamodel.ARRAY,
	pgtype.VarbitOID:           schemamodel.STRING,
	pgtype.VarbitArrayOID:      schemamodel.ARRAY,
	1266:                       schemamodel.STRING, // timetz
	1270:                       schemamodel.ARRAY,  // timetz array
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
	pgtype.Float4OID:           float42float,
	pgtype.Float4ArrayOID:      arrayConverter[[]float32](pgtype.Float4OID, float42float),
	pgtype.Float8OID:           float82float,
	pgtype.Float8ArrayOID:      arrayConverter[[]float64](pgtype.Float8OID, float82float),
	pgtype.BPCharOID:           nil,
	pgtype.QCharOID:            char2text,
	1002:                       arrayConverter[[]string](pgtype.QCharOID, char2text), // QCharArrayOID
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
	pgtype.ByteaOID:            bytes2hexstring,
	pgtype.ByteaArrayOID:       arrayConverter[[]string](pgtype.ByteaOID, bytes2hexstring),
	pgtype.JSONOID:             json2text,
	pgtype.JSONArrayOID:        arrayConverter[[]string](pgtype.JSONOID, json2text),
	pgtype.JSONBOID:            json2text,
	pgtype.JSONBArrayOID:       arrayConverter[[]string](pgtype.JSONBOID, json2text),
	pgtype.UUIDOID:             uuid2text,
	pgtype.UUIDArrayOID:        arrayConverter[[]string](pgtype.UUIDOID, uuid2text),
	pgtype.NameOID:             nil,
	pgtype.NameArrayOID:        arrayConverter[[]string](pgtype.NameOID, nil),
	pgtype.OIDOID:              uint322int64,
	pgtype.OIDArrayOID:         arrayConverter[[]int64](pgtype.OIDOID, uint322int64),
	pgtype.XIDOID:              uint322int64,
	pgtype.XIDArrayOID:         arrayConverter[[]int64](pgtype.XIDOID, uint322int64),
	pgtype.CIDOID:              uint322int64,
	pgtype.CIDArrayOID:         arrayConverter[[]int64](pgtype.CIDOID, uint322int64),
	pgtype.CIDROID:             addr2text,
	pgtype.CIDRArrayOID:        arrayConverter[[]string](pgtype.CIDROID, addr2text),
	pgtype.MacaddrOID:          macaddr2text,
	pgtype.MacaddrArrayOID:     arrayConverter[[]string](pgtype.MacaddrOID, macaddr2text),
	774:                        macaddr2text,                                // macaddr8
	775:                        arrayConverter[[]string](774, macaddr2text), // macaddr8[]
	pgtype.InetOID:             addr2text,
	pgtype.InetArrayOID:        arrayConverter[[]string](pgtype.InetOID, addr2text),
	pgtype.DateOID:             date2int32,
	pgtype.DateArrayOID:        arrayConverter[[]int32](pgtype.DateOID, date2int32),
	pgtype.TimeOID:             time2text,
	pgtype.TimeArrayOID:        arrayConverter[[]string](pgtype.TimeOID, time2text),
	pgtype.NumericOID:          numeric2float64,
	pgtype.NumericArrayOID:     arrayConverter[[]float64](pgtype.NumericOID, numeric2float64),
	pgtype.Int4rangeOID:        intrange2string,
	pgtype.Int4rangeArrayOID:   arrayConverter[[]string](pgtype.Int4rangeOID, intrange2string),
	pgtype.Int8rangeOID:        intrange2string,
	pgtype.Int8rangeArrayOID:   arrayConverter[[]string](pgtype.Int8rangeOID, intrange2string),
	pgtype.NumrangeOID:         numrange2string,
	pgtype.NumrangeArrayOID:    arrayConverter[[]string](pgtype.NumrangeOID, numrange2string),
	pgtype.TsrangeOID:          timestamprange2string,
	pgtype.TsrangeArrayOID:     arrayConverter[[]string](pgtype.TsrangeOID, timestamprange2string),
	pgtype.TstzrangeOID:        timestamprange2string,
	pgtype.TstzrangeArrayOID:   arrayConverter[[]string](pgtype.TstzrangeOID, timestamprange2string),
	pgtype.DaterangeOID:        timestamprange2string,
	pgtype.DaterangeArrayOID:   arrayConverter[[]string](pgtype.DaterangeOID, timestamprange2string),
	pgtype.BitOID:              bits2string,
	pgtype.BitArrayOID:         arrayConverter[[]string](pgtype.BitOID, bits2string),
	pgtype.VarbitOID:           bits2string,
	pgtype.VarbitArrayOID:      arrayConverter[[]string](pgtype.VarbitOID, bits2string),
	1266:                       time2text,                                 // timetz
	1270:                       arrayConverter[[]string](1266, time2text), // timetz array
}

var optimizedTypes = map[string]typeRegistration{
	/*"geometry": {
		schemaType: schemamodel.STRUCT,
		converter: nil,
	},
	"_geometry": {
		schemaType: schemamodel.ARRAY,
		isArray:    true,
	},*/
	"ltree": {
		schemaType: schemamodel.STRING,
		converter:  ltree2string,
		codec:      pgdecoding.LtreeCodec{},
	},
	"_ltree": {
		schemaType: schemamodel.ARRAY,
		isArray:    true,
	},
}

// ErrIllegalValue represents an illegal type conversion request
// for the given value
var ErrIllegalValue = fmt.Errorf("illegal value for data type conversion")

type TypeManager struct {
	logger              *logging.Logger
	typeResolver        TypeResolver
	typeCache           map[uint32]systemcatalog.PgType
	typeNameCache       map[string]uint32
	typeCacheMutex      sync.Mutex
	optimizedTypes      map[uint32]systemcatalog.PgType
	optimizedConverters map[uint32]typeRegistration
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
		typeNameCache:       make(map[string]uint32),
		typeCacheMutex:      sync.Mutex{},
		optimizedTypes:      make(map[uint32]systemcatalog.PgType),
		optimizedConverters: make(map[uint32]typeRegistration),
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
		tm.typeNameCache[typ.Name()] = typ.Oid()

		if registration, present := optimizedTypes[typ.Name()]; present {
			if t, ok := typ.(*pgType); ok {
				t.schemaType = registration.schemaType
			}

			tm.optimizedTypes[typ.Oid()] = typ

			var converter Converter
			if registration.isArray {
				lazyConverter := &lazyArrayConverter{
					typeManager: tm,
					oidElement:  typ.OidElement(),
				}
				converter = lazyConverter.convert
			} else {
				converter = registration.converter
			}

			if converter == nil {
				return errors.Errorf("Type %s has no assigned value converter", typ.Name())
			}

			tm.optimizedConverters[typ.Oid()] = typeRegistration{
				schemaType: registration.schemaType,
				isArray:    registration.isArray,
				oidElement: typ.OidElement(),
				converter:  converter,
				codec:      registration.codec,
			}

			if typ.IsArray() {
				if elementType, present := pgdecoding.GetType(typ.OidElement()); present {
					pgdecoding.RegisterType(&pgtype.Type{
						Name: typ.Name(), OID: typ.Oid(), Codec: &pgtype.ArrayCodec{ElementType: elementType},
					})
				}
			} else {
				pgdecoding.RegisterType(&pgtype.Type{Name: typ.Name(), OID: typ.Oid(), Codec: registration.codec})
			}
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

	pgType := newType(tm, name, kind, oid, category, arrayType, recordType,
		oidArray, oidElement, oidParent, modifiers, enumValues, delimiter)

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
	if registration, present := tm.optimizedConverters[oid]; present {
		return registration.converter, nil
	}
	return nil, fmt.Errorf("unsupported OID: %d", oid)
}

func (tm *TypeManager) NumKnownTypes() int {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()
	return len(tm.typeCache)
}

func (tm *TypeManager) OidByName(name string) uint32 {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()
	oid, present := tm.typeNameCache[name]
	if !present {
		panic(fmt.Sprintf("Type %s isn't registered"))
	}
	return oid
}

func (tm *TypeManager) getSchemaType(oid uint32, arrayType bool, kind systemcatalog.PgKind) schemamodel.SchemaType {
	if coreType, present := coreTypes[oid]; present {
		return coreType
	}
	if registration, present := tm.optimizedConverters[oid]; present {
		return registration.schemaType
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

type lazyArrayConverter struct {
	typeManager *TypeManager
	oidElement  uint32
	converter   Converter
}

func (lac *lazyArrayConverter) convert(oid uint32, value any) (any, error) {
	if lac.converter == nil {
		elementType, err := lac.typeManager.DataType(lac.oidElement)
		if err != nil {
			return nil, err
		}

		elementConverter, err := lac.typeManager.Converter(lac.oidElement)
		if err != nil {
			return nil, err
		}

		reflectiveType, err := schemaType2ReflectiveType(elementType.SchemaType())
		if err != nil {
			return nil, err
		}

		targetType := reflect.SliceOf(reflectiveType)
		lac.converter = reflectiveArrayConverter(lac.oidElement, targetType, elementConverter)
	}

	return lac.converter(oid, value)
}

func schemaType2ReflectiveType(schemaType schemamodel.SchemaType) (reflect.Type, error) {
	switch schemaType {
	case schemamodel.INT8:
		return int8Type, nil
	case schemamodel.INT16:
		return int16Type, nil
	case schemamodel.INT32:
		return int32Type, nil
	case schemamodel.INT64:
		return int64Type, nil
	case schemamodel.FLOAT32:
		return float32Type, nil
	case schemamodel.FLOAT64:
		return float64Type, nil
	case schemamodel.BOOLEAN:
		return booleanType, nil
	case schemamodel.STRING:
		return stringType, nil
	case schemamodel.BYTES:
		return byteaType, nil
	case schemamodel.MAP:
		return mapType, nil
	default:
		return nil, errors.Errorf("Unsupported schema type %s", string(schemaType))
	}
}
