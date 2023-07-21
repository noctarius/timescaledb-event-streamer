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

type TypeFactory func(namespace, name string, kind systemcatalog.PgKind, oid uint32, category systemcatalog.PgCategory,
	arrayType bool, recordType bool, oidArray uint32, oidElement uint32, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) systemcatalog.PgType

type TypeResolver interface {
	ReadPgTypes(factory TypeFactory, callback func(systemcatalog.PgType) error) error
	ReadPgType(oid uint32, factory TypeFactory) (systemcatalog.PgType, bool, error)
}

type typeRegistration struct {
	schemaType    schemamodel.Type
	schemaBuilder schemamodel.SchemaBuilder
	isArray       bool
	oidElement    uint32
	converter     Converter
	codec         pgtype.Codec
}

var coreTypes = map[uint32]typeRegistration{
	pgtype.BoolOID: {
		schemaType: schemamodel.BOOLEAN,
	},
	pgtype.BoolArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.BoolOID,
	},
	pgtype.Int2OID: {
		schemaType: schemamodel.INT16,
	},
	pgtype.Int2ArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Int2OID,
	},
	pgtype.Int4OID: {
		schemaType: schemamodel.INT32,
	},
	pgtype.Int4ArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Int4OID,
	},
	pgtype.Int8OID: {
		schemaType: schemamodel.INT64,
	},
	pgtype.Int8ArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Int8OID,
	},
	pgtype.Float4OID: {
		schemaType: schemamodel.FLOAT32,
		converter:  float42float,
	},
	pgtype.Float4ArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Float4OID,
		converter:  arrayConverter[[]float32](pgtype.Float4OID, float42float),
	},
	pgtype.Float8OID: {
		schemaType: schemamodel.FLOAT64,
		converter:  float82float,
	},
	pgtype.Float8ArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Float8OID,
		converter:  arrayConverter[[]float64](pgtype.Float8OID, float82float),
	},
	pgtype.BPCharOID: {
		schemaType: schemamodel.STRING,
	},
	pgtype.BPCharArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.BPCharOID,
	},
	pgtype.QCharOID: {
		schemaType: schemamodel.STRING,
		converter:  char2text,
	},
	QCharArrayOID: { // QCharArrayOID
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.QCharOID,
		converter:  arrayConverter[[]string](pgtype.QCharOID, char2text),
	},
	pgtype.VarcharOID: {
		schemaType: schemamodel.STRING,
	},
	pgtype.VarcharArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.VarcharOID,
	},
	pgtype.TextOID: {
		schemaType: schemamodel.STRING,
	},
	pgtype.TextArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.TextOID,
	},
	pgtype.TimestampOID: {
		schemaType: schemamodel.INT64,
		converter:  timestamp2int64,
	},
	pgtype.TimestampArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.TimestampOID,
		converter:  arrayConverter[[]int64](pgtype.TimestampOID, timestamp2int64),
	},
	pgtype.TimestamptzOID: {
		schemaType: schemamodel.STRING,
		converter:  timestamp2text,
	},
	pgtype.TimestamptzArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.TimestamptzOID,
		converter:  arrayConverter[[]string](pgtype.TimestamptzOID, timestamp2text),
	},
	pgtype.IntervalOID: {
		schemaType: schemamodel.INT64,
		converter:  interval2int64,
	},
	pgtype.IntervalArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.IntervalOID,
		converter:  arrayConverter[[]int64](pgtype.IntervalOID, interval2int64),
	},
	pgtype.ByteaOID: {
		schemaType: schemamodel.STRING,
		converter:  bytes2hexstring,
	},
	pgtype.ByteaArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.ByteaOID,
		converter:  arrayConverter[[]string](pgtype.ByteaOID, bytes2hexstring),
	},
	pgtype.JSONOID: {
		schemaType: schemamodel.STRING,
		converter:  json2text,
	},
	pgtype.JSONArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.JSONOID,
		converter:  arrayConverter[[]string](pgtype.JSONOID, json2text),
	},
	pgtype.JSONBOID: {
		schemaType: schemamodel.STRING,
		converter:  json2text,
	},
	pgtype.JSONBArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.JSONBOID,
		converter:  arrayConverter[[]string](pgtype.JSONBOID, json2text),
	},
	pgtype.UUIDOID: {
		schemaType: schemamodel.STRING,
		converter:  uuid2text,
	},
	pgtype.UUIDArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.UUIDOID,
		converter:  arrayConverter[[]string](pgtype.UUIDOID, uuid2text),
	},
	pgtype.NameOID: {
		schemaType: schemamodel.STRING,
	},
	pgtype.NameArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.NameOID,
	},
	pgtype.OIDOID: {
		schemaType: schemamodel.INT64,
		converter:  uint322int64,
	},
	pgtype.OIDArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.OIDOID,
		converter:  arrayConverter[[]int64](pgtype.OIDOID, uint322int64),
	},
	pgtype.XIDOID: {
		schemaType: schemamodel.INT64,
		converter:  uint322int64,
	},
	pgtype.XIDArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.XIDOID,
		converter:  arrayConverter[[]int64](pgtype.XIDOID, uint322int64),
	},
	pgtype.CIDOID: {
		schemaType: schemamodel.INT64,
		converter:  uint322int64,
	},
	pgtype.CIDArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.CIDOID,
		converter:  arrayConverter[[]int64](pgtype.CIDOID, uint322int64),
	},
	pgtype.CIDROID: {
		schemaType: schemamodel.STRING,
		converter:  addr2text,
	},
	pgtype.CIDRArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.CIDROID,
		converter:  arrayConverter[[]string](pgtype.CIDROID, addr2text),
	},
	pgtype.MacaddrOID: {
		schemaType: schemamodel.STRING,
		converter:  macaddr2text,
	},
	pgtype.MacaddrArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.MacaddrOID,
		converter:  arrayConverter[[]string](pgtype.MacaddrOID, macaddr2text),
	},
	MacAddr8OID: {
		schemaType: schemamodel.STRING,
		converter:  macaddr2text,
	},
	MacAddrArray8OID: {
		schemaType: schemamodel.ARRAY,
		oidElement: MacAddr8OID,
		converter:  arrayConverter[[]string](MacAddr8OID, macaddr2text),
	},
	pgtype.InetOID: {
		schemaType: schemamodel.STRING,
		converter:  addr2text,
	},
	pgtype.InetArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.InetOID,
		converter:  arrayConverter[[]string](pgtype.InetOID, addr2text),
	},
	pgtype.DateOID: {
		schemaType: schemamodel.INT32,
		converter:  date2int32,
	},
	pgtype.DateArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.DateOID,
		converter:  arrayConverter[[]int32](pgtype.DateOID, date2int32),
	},
	pgtype.TimeOID: {
		schemaType: schemamodel.STRING,
		converter:  time2text,
	},
	pgtype.TimeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.TimeOID,
		converter:  arrayConverter[[]string](pgtype.TimeOID, time2text),
	},
	pgtype.NumericOID: {
		schemaType: schemamodel.FLOAT64,
		converter:  numeric2float64,
	},
	pgtype.NumericArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.NumericOID,
		converter:  arrayConverter[[]float64](pgtype.NumericOID, numeric2float64),
	},
	pgtype.Int4rangeOID: {
		schemaType: schemamodel.STRING,
		converter:  intrange2string,
	},
	pgtype.Int4rangeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Int4rangeOID,
		converter:  arrayConverter[[]string](pgtype.Int4rangeOID, intrange2string),
	},
	pgtype.Int8rangeOID: {
		schemaType: schemamodel.STRING,
		converter:  intrange2string,
	},
	pgtype.Int8rangeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.Int8rangeOID,
		converter:  arrayConverter[[]string](pgtype.Int8rangeOID, intrange2string),
	},
	pgtype.NumrangeOID: {
		schemaType: schemamodel.STRING,
		converter:  numrange2string,
	},
	pgtype.NumrangeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.NumrangeOID,
		converter:  arrayConverter[[]string](pgtype.NumrangeOID, numrange2string),
	},
	pgtype.TsrangeOID: {
		schemaType: schemamodel.STRING,
		converter:  timestamprange2string,
	},
	pgtype.TsrangeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.TsrangeOID,
		converter:  arrayConverter[[]string](pgtype.TsrangeOID, timestamprange2string),
	},
	pgtype.TstzrangeOID: {
		schemaType: schemamodel.STRING,
		converter:  timestamprange2string,
	},
	pgtype.TstzrangeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.TstzrangeOID,
		converter:  arrayConverter[[]string](pgtype.TstzrangeOID, timestamprange2string),
	},
	pgtype.DaterangeOID: {
		schemaType: schemamodel.STRING,
		converter:  timestamprange2string,
	},
	pgtype.DaterangeArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.DaterangeOID,
		converter:  arrayConverter[[]string](pgtype.DaterangeOID, timestamprange2string),
	},
	pgtype.BitOID: {
		schemaType: schemamodel.STRING,
		converter:  bits2string,
	},
	pgtype.BitArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.BitOID,
		converter:  arrayConverter[[]string](pgtype.BitOID, bits2string),
	},
	pgtype.VarbitOID: {
		schemaType: schemamodel.STRING,
		converter:  bits2string,
	},
	pgtype.VarbitArrayOID: {
		schemaType: schemamodel.ARRAY,
		oidElement: pgtype.VarbitOID,
		converter:  arrayConverter[[]string](pgtype.VarbitOID, bits2string),
	},
	TimeTZOID: { // timetz
		schemaType: schemamodel.STRING,
		converter:  time2text,
	},
	TimeTZArrayOID: { // timetz[]
		schemaType: schemamodel.ARRAY,
		oidElement: TimeTZOID,
		converter:  arrayConverter[[]string](TimeTZOID, time2text),
	},
	XmlOID: { // xml
		schemaType: schemamodel.STRING,
	},
	XmlArrayOID: { // timetz[]
		schemaType: schemamodel.ARRAY,
		oidElement: XmlOID,
	},
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
		schemaType:    schemamodel.STRING,
		schemaBuilder: schemamodel.Ltree(),
		converter:     ltree2string,
		codec:         pgdecoding.LtreeCodec{},
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

	coreTypesSlice := supporting.MapMapper(coreTypes, func(key uint32, _ typeRegistration) uint32 {
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
				schemaType:    registration.schemaType,
				schemaBuilder: registration.schemaBuilder,
				isArray:       registration.isArray,
				oidElement:    typ.OidElement(),
				converter:     converter,
				codec:         registration.codec,
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

func (tm *TypeManager) typeFactory(namespace, name string, kind systemcatalog.PgKind, oid uint32,
	category systemcatalog.PgCategory, arrayType bool, recordType bool, oidArray uint32, oidElement uint32,
	oidParent uint32, modifiers int, enumValues []string, delimiter string) systemcatalog.PgType {

	return newType(tm, namespace, name, kind, oid, category, arrayType, recordType,
		oidArray, oidElement, oidParent, modifiers, enumValues, delimiter)
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

func (tm *TypeManager) getSchemaType(oid uint32, arrayType bool, kind systemcatalog.PgKind) schemamodel.Type {
	if registration, present := coreTypes[oid]; present {
		return registration.schemaType
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

func (tm *TypeManager) resolveSchemaBuilder(pgType *pgType) schemamodel.SchemaBuilder {
	if registration, present := tm.optimizedConverters[pgType.oid]; present {
		if registration.schemaBuilder != nil {
			return registration.schemaBuilder
		}
	}

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
		if pgType.kind == systemcatalog.EnumKind {
			return schemamodel.Enum(pgType.EnumValues())
		}
		switch pgType.oid {
		case pgtype.JSONOID, pgtype.JSONBOID:
			return schemamodel.Json()

		case pgtype.UUIDOID:
			return schemamodel.Uuid()

		case pgtype.BitOID:
			// TODO: needs better handling

		case 142: // XML
			return schemamodel.Xml()
		}
		return schemamodel.String()

	case schemamodel.BYTES:
		return schemamodel.Bytes()

	case schemamodel.ARRAY:
		elementType := pgType.ElementType()
		return schemamodel.NewSchemaBuilder(pgType.schemaType).ValueSchema(elementType.SchemaBuilder())

	case schemamodel.MAP:
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

func schemaType2ReflectiveType(schemaType schemamodel.Type) (reflect.Type, error) {
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
