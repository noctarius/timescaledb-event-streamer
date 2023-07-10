/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package systemcatalog

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"net"
	"net/netip"
	"time"
)

// DataType is a string like definition of the available
// event stream data types
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
	//pgtype.NumericOID:     BYTES,
}

var (
	converters = map[uint32]Converter{
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
		//pgtype.NumericOID:     nil,
	}
)

// ErrIllegalValue represents an illegal type conversion request
// for the given value
var ErrIllegalValue = fmt.Errorf("illegal value for data type conversion")

// Converter represents a conversion function to convert from
// a PostgreSQL internal OID number and value to a value according
// to the stream definition
type Converter func(oid uint32, value any) (any, error)

// DataTypeByOID returns the DataType for a given OID, if
// no valid mapping is available, it will return an error
func DataTypeByOID(oid uint32) (DataType, error) {
	if v, ok := mapping[oid]; ok {
		return v, nil
	}
	return "", fmt.Errorf("unsupported OID: %d", oid)
}

// ConverterByOID returns the Converter for a given OID, if
// no valid mapping is available, it will return an error
func ConverterByOID(oid uint32) (Converter, error) {
	if v, ok := converters[oid]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("unsupported OID: %d", oid)
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
