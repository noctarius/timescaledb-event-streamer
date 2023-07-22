package pgtypes

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
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

type TypeFactory func(namespace, name string, kind PgKind, oid uint32, category PgCategory,
	arrayType bool, recordType bool, oidArray uint32, oidElement uint32, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) PgType

type TypeResolver interface {
	ReadPgTypes(factory TypeFactory, callback func(PgType) error) error
	ReadPgType(oid uint32, factory TypeFactory) (PgType, bool, error)
}

type typeRegistration struct {
	schemaType    schema.Type
	schemaBuilder schema.SchemaBuilder
	isArray       bool
	oidElement    uint32
	converter     Converter
	codec         pgtype.Codec
}

var coreTypes = map[uint32]typeRegistration{
	pgtype.BoolOID: {
		schemaType: schema.BOOLEAN,
	},
	pgtype.BoolArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.BoolOID,
	},
	pgtype.Int2OID: {
		schemaType: schema.INT16,
	},
	pgtype.Int2ArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Int2OID,
	},
	pgtype.Int4OID: {
		schemaType: schema.INT32,
	},
	pgtype.Int4ArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Int4OID,
	},
	pgtype.Int8OID: {
		schemaType: schema.INT64,
	},
	pgtype.Int8ArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Int8OID,
	},
	pgtype.Float4OID: {
		schemaType: schema.FLOAT32,
		converter:  float42float,
	},
	pgtype.Float4ArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Float4OID,
		converter:  arrayConverter[[]float32](pgtype.Float4OID, float42float),
	},
	pgtype.Float8OID: {
		schemaType: schema.FLOAT64,
		converter:  float82float,
	},
	pgtype.Float8ArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Float8OID,
		converter:  arrayConverter[[]float64](pgtype.Float8OID, float82float),
	},
	pgtype.BPCharOID: {
		schemaType: schema.STRING,
	},
	pgtype.BPCharArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.BPCharOID,
	},
	pgtype.QCharOID: {
		schemaType: schema.STRING,
		converter:  char2text,
	},
	QCharArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.QCharOID,
		converter:  arrayConverter[[]string](pgtype.QCharOID, char2text),
	},
	pgtype.VarcharOID: {
		schemaType: schema.STRING,
	},
	pgtype.VarcharArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.VarcharOID,
	},
	pgtype.TextOID: {
		schemaType: schema.STRING,
	},
	pgtype.TextArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.TextOID,
	},
	pgtype.TimestampOID: {
		schemaType: schema.INT64,
		converter:  timestamp2int64,
	},
	pgtype.TimestampArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.TimestampOID,
		converter:  arrayConverter[[]int64](pgtype.TimestampOID, timestamp2int64),
	},
	pgtype.TimestamptzOID: {
		schemaType: schema.STRING,
		converter:  timestamp2text,
	},
	pgtype.TimestamptzArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.TimestamptzOID,
		converter:  arrayConverter[[]string](pgtype.TimestamptzOID, timestamp2text),
	},
	pgtype.IntervalOID: {
		schemaType: schema.INT64,
		converter:  interval2int64,
	},
	pgtype.IntervalArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.IntervalOID,
		converter:  arrayConverter[[]int64](pgtype.IntervalOID, interval2int64),
	},
	pgtype.ByteaOID: {
		schemaType: schema.STRING,
		converter:  bytes2hexstring,
	},
	pgtype.ByteaArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.ByteaOID,
		converter:  arrayConverter[[]string](pgtype.ByteaOID, bytes2hexstring),
	},
	pgtype.JSONOID: {
		schemaType: schema.STRING,
		converter:  json2text,
	},
	pgtype.JSONArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.JSONOID,
		converter:  arrayConverter[[]string](pgtype.JSONOID, json2text),
	},
	pgtype.JSONBOID: {
		schemaType: schema.STRING,
		converter:  json2text,
	},
	pgtype.JSONBArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.JSONBOID,
		converter:  arrayConverter[[]string](pgtype.JSONBOID, json2text),
	},
	pgtype.UUIDOID: {
		schemaType: schema.STRING,
		converter:  uuid2text,
	},
	pgtype.UUIDArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.UUIDOID,
		converter:  arrayConverter[[]string](pgtype.UUIDOID, uuid2text),
	},
	pgtype.NameOID: {
		schemaType: schema.STRING,
	},
	pgtype.NameArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.NameOID,
	},
	pgtype.OIDOID: {
		schemaType: schema.INT64,
		converter:  uint322int64,
	},
	pgtype.OIDArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.OIDOID,
		converter:  arrayConverter[[]int64](pgtype.OIDOID, uint322int64),
	},
	pgtype.XIDOID: {
		schemaType: schema.INT64,
		converter:  uint322int64,
	},
	pgtype.XIDArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.XIDOID,
		converter:  arrayConverter[[]int64](pgtype.XIDOID, uint322int64),
	},
	pgtype.CIDOID: {
		schemaType: schema.INT64,
		converter:  uint322int64,
	},
	pgtype.CIDArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.CIDOID,
		converter:  arrayConverter[[]int64](pgtype.CIDOID, uint322int64),
	},
	pgtype.CIDROID: {
		schemaType: schema.STRING,
		converter:  addr2text,
	},
	pgtype.CIDRArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.CIDROID,
		converter:  arrayConverter[[]string](pgtype.CIDROID, addr2text),
	},
	pgtype.MacaddrOID: {
		schemaType: schema.STRING,
		converter:  macaddr2text,
	},
	pgtype.MacaddrArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.MacaddrOID,
		converter:  arrayConverter[[]string](pgtype.MacaddrOID, macaddr2text),
	},
	MacAddr8OID: {
		schemaType: schema.STRING,
		converter:  macaddr2text,
	},
	MacAddrArray8OID: {
		schemaType: schema.ARRAY,
		oidElement: MacAddr8OID,
		converter:  arrayConverter[[]string](MacAddr8OID, macaddr2text),
	},
	pgtype.InetOID: {
		schemaType: schema.STRING,
		converter:  addr2text,
	},
	pgtype.InetArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.InetOID,
		converter:  arrayConverter[[]string](pgtype.InetOID, addr2text),
	},
	pgtype.DateOID: {
		schemaType: schema.INT32,
		converter:  date2int32,
	},
	pgtype.DateArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.DateOID,
		converter:  arrayConverter[[]int32](pgtype.DateOID, date2int32),
	},
	pgtype.TimeOID: {
		schemaType: schema.STRING,
		converter:  time2text,
	},
	pgtype.TimeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.TimeOID,
		converter:  arrayConverter[[]string](pgtype.TimeOID, time2text),
	},
	pgtype.NumericOID: {
		schemaType: schema.FLOAT64,
		converter:  numeric2float64,
	},
	pgtype.NumericArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.NumericOID,
		converter:  arrayConverter[[]float64](pgtype.NumericOID, numeric2float64),
	},
	pgtype.Int4rangeOID: {
		schemaType: schema.STRING,
		converter:  intrange2string,
	},
	pgtype.Int4rangeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Int4rangeOID,
		converter:  arrayConverter[[]string](pgtype.Int4rangeOID, intrange2string),
	},
	pgtype.Int8rangeOID: {
		schemaType: schema.STRING,
		converter:  intrange2string,
	},
	pgtype.Int8rangeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.Int8rangeOID,
		converter:  arrayConverter[[]string](pgtype.Int8rangeOID, intrange2string),
	},
	pgtype.NumrangeOID: {
		schemaType: schema.STRING,
		converter:  numrange2string,
	},
	pgtype.NumrangeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.NumrangeOID,
		converter:  arrayConverter[[]string](pgtype.NumrangeOID, numrange2string),
	},
	pgtype.TsrangeOID: {
		schemaType: schema.STRING,
		converter:  timestamprange2string,
	},
	pgtype.TsrangeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.TsrangeOID,
		converter:  arrayConverter[[]string](pgtype.TsrangeOID, timestamprange2string),
	},
	pgtype.TstzrangeOID: {
		schemaType: schema.STRING,
		converter:  timestamprange2string,
	},
	pgtype.TstzrangeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.TstzrangeOID,
		converter:  arrayConverter[[]string](pgtype.TstzrangeOID, timestamprange2string),
	},
	pgtype.DaterangeOID: {
		schemaType: schema.STRING,
		converter:  timestamprange2string,
	},
	pgtype.DaterangeArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.DaterangeOID,
		converter:  arrayConverter[[]string](pgtype.DaterangeOID, timestamprange2string),
	},
	pgtype.BitOID: {
		schemaType: schema.STRING,
		converter:  bits2string,
	},
	pgtype.BitArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.BitOID,
		converter:  arrayConverter[[]string](pgtype.BitOID, bits2string),
	},
	pgtype.VarbitOID: {
		schemaType: schema.STRING,
		converter:  bits2string,
	},
	pgtype.VarbitArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.VarbitOID,
		converter:  arrayConverter[[]string](pgtype.VarbitOID, bits2string),
	},
	TimeTZOID: {
		schemaType: schema.STRING,
		converter:  time2text,
	},
	TimeTZArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: TimeTZOID,
		converter:  arrayConverter[[]string](TimeTZOID, time2text),
	},
	XmlOID: {
		schemaType: schema.STRING,
	},
	XmlArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: XmlOID,
	},
}

var optimizedTypes = map[string]typeRegistration{
	/*"geometry": {
		schemaType: schema.STRUCT,
		converter: nil,
	},
	"_geometry": {
		schemaType: schema.ARRAY,
		isArray:    true,
	},*/
	"ltree": {
		schemaType:    schema.STRING,
		schemaBuilder: schema.Ltree(),
		converter:     ltree2string,
		codec:         LtreeCodec{},
	},
	"_ltree": {
		schemaType: schema.ARRAY,
		isArray:    true,
	},
}

// ErrIllegalValue represents an illegal type conversion request
// for the given value
var ErrIllegalValue = fmt.Errorf("illegal value for data type conversion")

type TypeManager struct {
	logger              *logging.Logger
	typeResolver        TypeResolver
	typeCache           map[uint32]PgType
	typeNameCache       map[string]uint32
	typeCacheMutex      sync.Mutex
	optimizedTypes      map[uint32]PgType
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
		typeCache:           make(map[uint32]PgType),
		typeNameCache:       make(map[string]uint32),
		typeCacheMutex:      sync.Mutex{},
		optimizedTypes:      make(map[uint32]PgType),
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

	coreTypesSlice := supporting.MapMapper(coreTypes, func(key uint32, _ typeRegistration) uint32 {
		return key
	})

	if err := tm.typeResolver.ReadPgTypes(tm.typeFactory, func(typ PgType) error {
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
				schemaType:    registration.schemaType,
				schemaBuilder: registration.schemaBuilder,
				isArray:       registration.isArray,
				oidElement:    typ.OidElement(),
				converter:     converter,
				codec:         registration.codec,
			}

			if typ.IsArray() {
				if elementType, present := GetType(typ.OidElement()); present {
					RegisterType(&pgtype.Type{
						Name: typ.Name(), OID: typ.Oid(), Codec: &pgtype.ArrayCodec{ElementType: elementType},
					})
				}
			} else {
				RegisterType(&pgtype.Type{Name: typ.Name(), OID: typ.Oid(), Codec: registration.codec})
			}
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (tm *TypeManager) typeFactory(namespace, name string, kind PgKind, oid uint32,
	category PgCategory, arrayType, recordType bool, oidArray uint32, oidElement uint32,
	oidParent uint32, modifiers int, enumValues []string, delimiter string) PgType {

	return newType(tm, namespace, name, kind, oid, category, arrayType, recordType,
		oidArray, oidElement, oidParent, modifiers, enumValues, delimiter)
}

func (tm *TypeManager) DataType(oid uint32) (PgType, error) {
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
	if registration, present := coreTypes[oid]; present {
		return registration.converter, nil
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
		panic(fmt.Sprintf("Type %s isn't registered", name))
	}
	return oid
}

func (tm *TypeManager) getSchemaType(oid uint32, arrayType bool, kind PgKind) schema.Type {
	if registration, present := coreTypes[oid]; present {
		return registration.schemaType
	}
	if registration, present := tm.optimizedConverters[oid]; present {
		return registration.schemaType
	}
	if arrayType {
		return schema.ARRAY
	} else if kind == EnumKind {
		return schema.STRING
	}
	return schema.STRUCT
}

func (tm *TypeManager) resolveSchemaBuilder(pgType *pgType) schema.SchemaBuilder {
	if registration, present := tm.optimizedConverters[pgType.oid]; present {
		if registration.schemaBuilder != nil {
			return registration.schemaBuilder
		}
	}

	switch pgType.schemaType {
	case schema.INT8:
		return schema.Int8()

	case schema.INT16:
		return schema.Int16()

	case schema.INT32:
		return schema.Int32()

	case schema.INT64:
		return schema.Int64()

	case schema.FLOAT32:
		return schema.Float32()

	case schema.FLOAT64:
		return schema.Float64()

	case schema.BOOLEAN:
		return schema.Boolean()

	case schema.STRING:
		if pgType.kind == EnumKind {
			return schema.Enum(pgType.EnumValues())
		}
		switch pgType.oid {
		case pgtype.JSONOID, pgtype.JSONBOID:
			return schema.Json()

		case pgtype.UUIDOID:
			return schema.Uuid()

		case pgtype.BitOID:
			// TODO: needs better handling

		case 142: // XML
			return schema.Xml()
		}
		return schema.String()

	case schema.BYTES:
		return schema.Bytes()

	case schema.ARRAY:
		elementType := pgType.ElementType()
		return schema.NewSchemaBuilder(pgType.schemaType).ValueSchema(elementType.SchemaBuilder())

	case schema.MAP:
		return nil

	default:
		return nil
	}
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

func schemaType2ReflectiveType(schemaType schema.Type) (reflect.Type, error) {
	switch schemaType {
	case schema.INT8:
		return int8Type, nil
	case schema.INT16:
		return int16Type, nil
	case schema.INT32:
		return int32Type, nil
	case schema.INT64:
		return int64Type, nil
	case schema.FLOAT32:
		return float32Type, nil
	case schema.FLOAT64:
		return float64Type, nil
	case schema.BOOLEAN:
		return booleanType, nil
	case schema.STRING:
		return stringType, nil
	case schema.BYTES:
		return byteaType, nil
	case schema.MAP:
		return mapType, nil
	default:
		return nil, errors.Errorf("Unsupported schema type %s", string(schemaType))
	}
}
