package systemcatalog

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"time"
)

type DataType string

const (
	INT8    DataType = "int8"
	INT16   DataType = "int16"
	INT32   DataType = "int32"
	INT64   DataType = "int64"
	FLOAT32 DataType = "float32"
	FLOAT64 DataType = "float64"
	BOOLEAN DataType = "boolean"
	STRING  DataType = "string"
	BYTES   DataType = "bytes"
	ARRAY   DataType = "array"
	MAP     DataType = "map"
	STRUCT  DataType = "struct"
)

var mapping = map[uint32]DataType{
	pgtype.BoolOID:        BOOLEAN,
	pgtype.BitOID:         BOOLEAN,
	pgtype.BitArrayOID:    BYTES,
	pgtype.Int2OID:        INT16,
	pgtype.Int4OID:        INT32,
	pgtype.Int8OID:        INT64,
	pgtype.Float4OID:      FLOAT32,
	pgtype.Float8OID:      FLOAT64,
	pgtype.QCharOID:       STRING,
	pgtype.VarcharOID:     STRING,
	pgtype.TextOID:        STRING,
	pgtype.TimestampOID:   INT64,
	pgtype.TimestamptzOID: STRING,
	pgtype.IntervalOID:    INT64,
	pgtype.ByteaArrayOID:  BYTES,
	pgtype.JSONOID:        STRING,
	pgtype.JSONBOID:       STRING,
	pgtype.UUIDOID:        STRING,
	pgtype.PointOID:       STRUCT,
	pgtype.NumericOID:     BYTES,
	514836:                STRUCT, // geometry
	516272:                STRUCT, // ltree
}

var converters = map[uint32]Converter{
	pgtype.BoolOID:        nil,
	pgtype.BitOID:         nil,
	pgtype.BitArrayOID:    bits2bytes,
	pgtype.Int2OID:        nil,
	pgtype.Int4OID:        nil,
	pgtype.Int8OID:        nil,
	pgtype.Float4OID:      nil,
	pgtype.Float8OID:      nil,
	pgtype.QCharOID:       nil,
	pgtype.VarcharOID:     nil,
	pgtype.TextOID:        nil,
	pgtype.TimestampOID:   timestamp2text,
	pgtype.TimestamptzOID: timestamp2text,
	pgtype.IntervalOID:    nil,
	pgtype.ByteaArrayOID:  nil,
	pgtype.JSONOID:        json2text,
	pgtype.JSONBOID:       json2text,
	pgtype.UUIDOID:        uuid2text,
	pgtype.PointOID:       nil, // FIXME
	pgtype.NumericOID:     nil, // FIXME
	514836:                nil, // geometry, FIXME
	516272:                nil, // ltree, FIXME
}

var ErrIllegalValue = fmt.Errorf("illegal value for data type conversion")

type Converter func(oid uint32, value any) (any, error)

func DataTypeByOID(oid uint32) (DataType, error) {
	if v, ok := mapping[oid]; ok {
		return v, nil
	}
	return "", fmt.Errorf("unsupported OID: %d", oid)
}

func ConverterByOID(oid uint32) (Converter, error) {
	if v, ok := converters[oid]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("unsupported OID: %d", oid)
}

func timestamp2text(_ uint32, value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		return v.UnixMilli(), nil
	}
	return nil, ErrIllegalValue
}

func bits2bytes(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Bits); ok {
		return v.Bytes, nil
	}
	return nil, ErrIllegalValue
}

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
	}
	return nil, ErrIllegalValue
}
