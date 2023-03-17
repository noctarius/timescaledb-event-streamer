package model

import (
	"fmt"
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

var mapping = map[int]DataType{
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

var converters = map[int]Converter{
	pgtype.TimestampOID: timestamp,
}

var ErrIllegalValue = fmt.Errorf("illegal value for data type conversion")

type Converter func(oid int, value any) (any, error)

func DataTypeByOID(oid int) (DataType, error) {
	if v, ok := mapping[oid]; ok {
		return v, nil
	}
	return "", fmt.Errorf("unsupported OID: %d", oid)
}

func timestamp(_ int, value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		return v.UnixMilli(), nil
	}
	return nil, ErrIllegalValue
}
