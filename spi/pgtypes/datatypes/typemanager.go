package datatypes

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hashicorp/go-uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"net"
	"net/netip"
	"sync"
	"time"
)

type SchemaBuilder interface {
	BaseSchemaType() SchemaType
	Schema(oid uint32, modifier int, value any) schemamodel.Struct
}

type TypeFactory func(name string, typ TypeType, oid uint32, category TypeCategory, arrayType bool, oidArray uint32,
	oidElement uint32, recordType bool, parentOid uint32, modifiers int, enumValues []string, delimiter string) Type

type TypeResolver interface {
	ReadPgTypes(factory TypeFactory, callback func(Type) error) error
	ReadPgType(oid uint32, factory TypeFactory) (Type, bool, error)
}

var coreTypes = map[uint32]SchemaType{
	pgtype.BoolOID:        BOOLEAN,
	pgtype.Int2OID:        INT16,
	pgtype.Int4OID:        INT32,
	pgtype.Int8OID:        INT64,
	pgtype.Float4OID:      FLOAT32,
	pgtype.Float8OID:      FLOAT64,
	pgtype.BPCharOID:      STRING,
	pgtype.QCharOID:       STRING,
	pgtype.VarcharOID:     STRING,
	pgtype.TextOID:        STRING,
	pgtype.TimestampOID:   INT64,
	pgtype.TimestamptzOID: STRING,
	pgtype.IntervalOID:    INT64,
	pgtype.ByteaOID:       BYTES,
	pgtype.JSONOID:        STRING,
	pgtype.JSONBOID:       STRING,
	pgtype.UUIDOID:        STRING,
	pgtype.NameOID:        STRING,
	pgtype.OIDOID:         INT64,
	pgtype.TIDOID:         INT64,
	pgtype.XIDOID:         INT64,
	pgtype.CIDOID:         INT64,
	pgtype.CIDROID:        STRING,
	pgtype.MacaddrOID:     STRING,
	774:                   STRING, //macaddr8
	pgtype.InetOID:        STRING,
	pgtype.DateOID:        STRING,
	pgtype.TimeOID:        STRING,
	pgtype.NumericOID:     STRUCT,
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

	nType := NewType(name, typ, oid, category, arrayType, oidArray, oidElement,
		recordType, parentOid, modifiers, enumValues, delimiter)

	nType.typeManager = tm
	return nType
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

func (tm *TypeManager) SchemaBuilder(oid uint32) SchemaBuilder {
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

func char2text(_ uint32, value any) (any, error) {
	if v, ok := value.(int32); ok {
		return string(v), nil
	}
	return nil, ErrIllegalValue
}

func timestamp2text(oid uint32, value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		switch oid {
		case pgtype.DateOID:
			return v.Format(time.DateOnly), nil
		default:
			return v.In(time.UTC).String(), nil
		}
	}
	return nil, ErrIllegalValue
}

func time2text(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Time); ok {
		remaining := int64(time.Microsecond) * v.Microseconds
		hours := remaining / int64(time.Hour)
		remaining = remaining % int64(time.Hour)
		minutes := remaining / int64(time.Minute)
		remaining = remaining % int64(time.Minute)
		seconds := remaining / int64(time.Second)
		remaining = remaining % int64(time.Second)
		return fmt.Sprintf(
			"%02d:%02d:%02d.%06d", hours, minutes, seconds,
			(time.Nanosecond * time.Duration(remaining)).Microseconds(),
		), nil
	}
	return nil, ErrIllegalValue
}

func timestamp2int64(_ uint32, value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		return v.UnixMilli(), nil
	}
	return nil, ErrIllegalValue
}

/*func bit2bool(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Bits); ok {
		return v.Bytes[0]&0xF0 == 128, nil
	}
	return nil, ErrIllegalValue
}

func bits2bytes(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Bits); ok {
		return v.Bytes, nil
	}
	return nil, ErrIllegalValue
}*/

func json2text(_ uint32, value any) (any, error) {
	if v, ok := value.(map[string]any); ok {
		d, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(d), nil
	}
	return nil, ErrIllegalValue
}

func uuid2text(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.UUID); ok {
		u, err := uuid.FormatUUID(v.Bytes[:])
		if err != nil {
			return nil, err
		}
		return u, nil
	} else if v, ok := value.([16]byte); ok {
		u, err := uuid.FormatUUID(v[:])
		if err != nil {
			return nil, err
		}
		return u, nil
	}
	return nil, ErrIllegalValue
}

func uint322int64(_ uint32, value any) (any, error) {
	if v, ok := value.(uint32); ok {
		return int64(v), nil
	}
	return nil, ErrIllegalValue
}

func macaddr2text(_ uint32, value any) (any, error) {
	if v, ok := value.(net.HardwareAddr); ok {
		return v.String(), nil
	}
	return nil, ErrIllegalValue
}

func addr2text(_ uint32, value any) (any, error) {
	if v, ok := value.(netip.Prefix); ok {
		return v.String(), nil
	}
	return nil, ErrIllegalValue
}

func interval2int64(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Interval); ok {
		return v.Microseconds, nil
	}
	return nil, ErrIllegalValue
}

func numeric2variableScaleDecimal(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Numeric); ok {
		return schemamodel.Struct{
			"value": hex.EncodeToString(v.Int.Bytes()),
			"scale": v.Exp,
		}, nil
	}
	return nil, ErrIllegalValue
}
