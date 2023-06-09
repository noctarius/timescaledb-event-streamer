package integration

import (
	stdctx "context"
	"encoding/base64"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	inttest "github.com/noctarius/timescaledb-event-streamer/internal/testing"
	"github.com/noctarius/timescaledb-event-streamer/internal/testing/testrunner"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"reflect"
	"strings"
	"testing"
	"time"
)

var dataTypeTable = []DataTypeTest{
	{
		name:           "Boolean",
		oid:            pgtype.BoolOID,
		pgTypeName:     "boolean",
		schemaTypeName: systemcatalog.BOOLEAN,
		value:          true,
		expected:       quickCheckValue[bool],
	},
	{
		name:           "Byte Array (bytea)",
		oid:            pgtype.ByteaOID,
		pgTypeName:     "bytea",
		schemaTypeName: systemcatalog.BYTES,
		value:          []byte{0xDE, 0xAD, 0xBE, 0xEF},
		expected:       checkByteArray,
	},
	{
		name:               "Quoted Char",
		oid:                pgtype.QCharOID,
		pgTypeName:         "\"char\"",
		columnNameOverride: "qchar",
		schemaTypeName:     systemcatalog.STRING,
		value:              'F',
		expected:           checkChar,
	},
	{
		name:           "PG Name",
		oid:            pgtype.NameOID,
		pgTypeName:     "name",
		schemaTypeName: systemcatalog.STRING,
		value:          "testname",
		expected:       quickCheckValue[string],
	},
	{
		name:           "Int (64bit)",
		oid:            pgtype.Int8OID,
		pgTypeName:     "int8",
		schemaTypeName: systemcatalog.INT64,
		value:          int64(64),
		expected:       quickCheckValue[int64],
	},
	{
		name:           "Int (16bit)",
		oid:            pgtype.Int2OID,
		pgTypeName:     "int2",
		schemaTypeName: systemcatalog.INT16,
		value:          int16(16),
		expected:       quickCheckValue[int16],
	},
	{
		name:           "Int (32bit)",
		oid:            pgtype.Int4OID,
		pgTypeName:     "int4",
		schemaTypeName: systemcatalog.INT32,
		value:          int32(32),
		expected:       quickCheckValue[int32],
	},
	{
		name:           "Text",
		oid:            pgtype.TextOID,
		pgTypeName:     "text",
		schemaTypeName: systemcatalog.STRING,
		value:          "Some Test Text",
		expected:       quickCheckValue[string],
	},
	{
		name:           "OID",
		oid:            pgtype.OIDOID,
		pgTypeName:     "oid",
		schemaTypeName: systemcatalog.INT64,
		value:          int64(123),
		expected:       quickCheckValue[int64],
	},
	{
		name:           "TID",
		oid:            pgtype.TIDOID,
		pgTypeName:     "tid",
		schemaTypeName: systemcatalog.STRUCT,
		value:          nil,
		expected:       quickCheckValue[int64],
		missingSupport: true,
	},
	{
		name:           "XID",
		oid:            pgtype.XIDOID,
		pgTypeName:     "xid",
		schemaTypeName: systemcatalog.INT64,
		value:          int64(123),
		expected:       quickCheckValue[int64],
	},
	{
		name:           "CID",
		oid:            pgtype.CIDOID,
		pgTypeName:     "cid",
		schemaTypeName: systemcatalog.INT64,
		value:          int64(123),
		expected:       quickCheckValue[int64],
	},
	{
		name:           "JSON",
		oid:            pgtype.JSONOID,
		pgTypeName:     "json",
		schemaTypeName: systemcatalog.STRING,
		value:          `{"foo":"bar"}`,
		expected:       quickCheckValue[string],
	},
	{
		name:           "JSON Array",
		oid:            pgtype.JSONArrayOID,
		pgTypeName:     "json[]",
		schemaTypeName: systemcatalog.ARRAY,
		value:          `{"foo":"bar"}`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Point",
		oid:            pgtype.PointOID,
		pgTypeName:     "point",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "LSEG",
		oid:            pgtype.LsegOID,
		pgTypeName:     "lseg",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Path",
		oid:            pgtype.PathOID,
		pgTypeName:     "path",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Box",
		oid:            pgtype.BoxOID,
		pgTypeName:     "box",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Polygon",
		oid:            pgtype.PolygonOID,
		pgTypeName:     "polygon",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Line",
		oid:            pgtype.LineOID,
		pgTypeName:     "line",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Line Array",
		oid:            pgtype.LineArrayOID,
		pgTypeName:     "line[]",
		schemaTypeName: systemcatalog.STRUCT,
		value:          `(1,2)`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "CIDR (IPv4)",
		oid:            pgtype.CIDROID,
		pgTypeName:     "cidr",
		schemaTypeName: systemcatalog.STRING,
		value:          `10.0.0.0/24`,
		expected:       quickCheckValue[string],
	},
	{
		name:           "CIDR (IPv4) Array",
		oid:            pgtype.CIDArrayOID,
		pgTypeName:     "cidr",
		schemaTypeName: systemcatalog.STRING,
		value:          `10.0.0.0/24`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "CIDR (IPv6)",
		oid:            pgtype.CIDROID,
		pgTypeName:     "cidr",
		schemaTypeName: systemcatalog.STRING,
		value:          `2001:4f8:3:ba::/64`,
		expected:       quickCheckValue[string],
	},
	{
		name:           "CIDR (IPv6) Array",
		oid:            pgtype.CIDArrayOID,
		pgTypeName:     "cidr",
		schemaTypeName: systemcatalog.STRING,
		value:          `::2001:4f8:3:ba::/64`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Float (32bit)",
		oid:            pgtype.Float4OID,
		pgTypeName:     "float4",
		schemaTypeName: systemcatalog.FLOAT32,
		value:          float32(13.1),
		expected:       quickCheckValue[float32],
	},
	{
		name:           "Float (64bit)",
		oid:            pgtype.Float8OID,
		pgTypeName:     "float8",
		schemaTypeName: systemcatalog.FLOAT64,
		value:          13.1,
		expected:       quickCheckValue[float64],
	},
	{
		name:           "Circle",
		oid:            pgtype.CircleOID,
		pgTypeName:     "circle",
		schemaTypeName: systemcatalog.STRUCT,
		value:          13.1,
		expected:       quickCheckValue[float64],
		missingSupport: true,
	},
	{
		name:           "Circle Array",
		oid:            pgtype.CircleArrayOID,
		pgTypeName:     "circle[]",
		schemaTypeName: systemcatalog.STRUCT,
		value:          13.1,
		expected:       quickCheckValue[float64],
		missingSupport: true,
	},
	{
		name:           "MAC Address",
		oid:            pgtype.MacaddrOID,
		pgTypeName:     "macaddr",
		schemaTypeName: systemcatalog.STRING,
		value:          "08:00:2b:01:02:03",
		expected:       quickCheckValue[string],
	},
	{
		name:           "MAC Address (EUI-64)",
		oid:            774,
		pgTypeName:     "macaddr8",
		schemaTypeName: systemcatalog.STRING,
		value:          "08:00:2b:01:02:03:04:05",
		expected:       quickCheckValue[string],
	},
	{
		name:           "MAC Address (EUI-64) Array",
		oid:            pgtype.MacaddrArrayOID,
		pgTypeName:     "macaddr8[]",
		schemaTypeName: systemcatalog.STRING,
		value:          "08:00:2b:01:02:03:04:05",
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
	{
		name:           "Inet (IPv4)",
		oid:            pgtype.InetOID,
		pgTypeName:     "inet",
		schemaTypeName: systemcatalog.STRING,
		value:          "127.0.0.1/32",
		expected:       quickCheckValue[string],
	},
	{
		name:           "Inet (IPv6)",
		oid:            pgtype.InetOID,
		pgTypeName:     "inet",
		schemaTypeName: systemcatalog.STRING,
		value:          "::1/128",
		expected:       quickCheckValue[string],
	},
	{
		name:                  "Boolean Array",
		oid:                   pgtype.BoolArrayOID,
		pgTypeName:            "boolean[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.BOOLEAN,
		value:                 []bool{true, false, true},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Quoted Char Array",
		oid:                   pgtype.QCharArrayOID,
		pgTypeName:            "\"char\"[]",
		columnNameOverride:    "qchar[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []int32{'F', 'T', 'O'},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "name Array",
		oid:                   pgtype.NameArrayOID,
		pgTypeName:            "name[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []int32{'F', 'T', 'O'},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Int (16bit) Array",
		oid:                   pgtype.Int2ArrayOID,
		pgTypeName:            "int2[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.INT16,
		value:                 []int16{5, 10, 15},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Int (32bit) Array",
		oid:                   pgtype.Int4ArrayOID,
		pgTypeName:            "int4[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.INT32,
		value:                 []int32{5, 10, 15},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Text Array",
		oid:                   pgtype.TextArrayOID,
		pgTypeName:            "text[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []string{"first", "second", "third"},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Bytea Array",
		oid:                   pgtype.ByteaArrayOID,
		pgTypeName:            "bytea[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.BYTES,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "XID Array",
		oid:                   pgtype.XIDArrayOID,
		pgTypeName:            "xid[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.BYTES,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "CID Array",
		oid:                   pgtype.XIDArrayOID,
		pgTypeName:            "cid[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.BYTES,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Fixed Length Char Array",
		oid:                   pgtype.BPCharArrayOID,
		pgTypeName:            "char(4)[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Varchar Array",
		oid:                   pgtype.VarcharArrayOID,
		pgTypeName:            "varchar[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Int (64bit) Array",
		oid:                   pgtype.Int8ArrayOID,
		pgTypeName:            "int8[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.INT64,
		value:                 []int64{5, 10, 15},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Point Array",
		oid:                   pgtype.PointArrayOID,
		pgTypeName:            "point[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "LSEG Array",
		oid:                   pgtype.LsegArrayOID,
		pgTypeName:            "lseg[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Path Array",
		oid:                   pgtype.PathArrayOID,
		pgTypeName:            "path[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Box Array",
		oid:                   pgtype.BoxArrayOID,
		pgTypeName:            "box[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Float (32bit) Array",
		oid:                   pgtype.Float4ArrayOID,
		pgTypeName:            "float4[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.FLOAT32,
		value:                 []float32{14.1, 12.7},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Float (64bit) Array",
		oid:                   pgtype.Float8ArrayOID,
		pgTypeName:            "float8[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.FLOAT64,
		value:                 []float64{14.1, 12.7},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Polygon Array",
		oid:                   pgtype.PolygonArrayOID,
		pgTypeName:            "polygon[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRUCT,
		value:                 []float64{14.1, 12.7},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "OID Array",
		oid:                   pgtype.OIDArrayOID,
		pgTypeName:            "oid[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRUCT,
		value:                 []float64{14.1, 12.7},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Macaddr Array",
		oid:                   pgtype.MacaddrArrayOID,
		pgTypeName:            "macaddr[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []string{"08:00:2b:01:02:03", "01:02:03:04:05:06"},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Macaddr8 Array",
		oid:                   775,
		pgTypeName:            "macaddr8[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []string{"08:00:2b:01:02:03:04:05", "01:02:03:04:05:06:07:08"},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Inet (IPv4) Array",
		oid:                   pgtype.InetArrayOID,
		pgTypeName:            "inet[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []string{"127.0.0.1/32", "192.168.196.1/24"},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:                  "Inet (IPv6) Array",
		oid:                   pgtype.InetArrayOID,
		pgTypeName:            "inet[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 []string{"::1/128", "2001:4f8:3:ba::1/64"},
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:               "Fixed Length Char",
		oid:                pgtype.BPCharOID,
		pgTypeName:         "char(3)",
		columnNameOverride: "bpchar",
		schemaTypeName:     systemcatalog.STRING,
		value:              "  F",
		expected:           quickCheckValue[string],
	},
	{
		name:           "Varchar",
		oid:            pgtype.VarbitOID,
		pgTypeName:     "varchar",
		schemaTypeName: systemcatalog.STRING,
		value:          "F",
		expected:       quickCheckValue[string],
	},
	{
		name:           "Date",
		oid:            pgtype.DateOID,
		pgTypeName:     "date",
		schemaTypeName: systemcatalog.STRING,
		value:          "2023-01-01",
		expected:       quickCheckValue[string],
	},
	{
		name:           "Time",
		oid:            pgtype.TimeOID,
		pgTypeName:     "time",
		schemaTypeName: systemcatalog.STRING,
		value:          "12:00:12.054321",
		expected:       quickCheckValue[string],
	},
	{
		name:           "Timestamp Without Timezone",
		oid:            pgtype.TimestampOID,
		pgTypeName:     "timestamp",
		schemaTypeName: systemcatalog.INT64,
		value:          "2023-01-01T12:00:12.054321",
		valueOverride:  int64(1672574412054),
		expected:       quickCheckValue[int64],
	},
	{
		name:                  "Timestamp Without Timezone Array",
		oid:                   pgtype.TimestampArrayOID,
		pgTypeName:            "timestamp[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.INT64,
		value:                 "2023-01-01T12:00:12.054321",
		valueOverride:         int64(1672574412054),
		expected:              quickCheckValue[int64],
		missingSupport:        true,
	},
	{
		name:                  "Date Array",
		oid:                   pgtype.DateArrayOID,
		pgTypeName:            "date[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 "2023-01-01T12:00:12.054321",
		valueOverride:         int64(1672574412054),
		expected:              quickCheckValue[int64],
		missingSupport:        true,
	},
	{
		name:                  "Time Array",
		oid:                   pgtype.TimeArrayOID,
		pgTypeName:            "time[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 "2023-01-01T12:00:12.054321",
		valueOverride:         int64(1672574412054),
		expected:              quickCheckValue[int64],
		missingSupport:        true,
	},
	{
		name:           "Timestamp With Timezone",
		oid:            pgtype.TimestamptzOID,
		pgTypeName:     "timestamptz",
		schemaTypeName: systemcatalog.STRING,
		value:          "2023-01-01T12:00:12.054321Z07:00",
		valueOverride:  "2023-01-01 19:00:12.054321 +0000 UTC",
		expected:       quickCheckValue[string],
	},
	{
		name:                  "Timestamp With Timezone Array",
		oid:                   pgtype.TimestamptzArrayOID,
		pgTypeName:            "timestamptz[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 "2023-01-01T12:00:12.054321Z07:00",
		valueOverride:         "2023-01-01 19:00:12.054321 +0000 UTC",
		expected:              quickCheckValue[string],
		missingSupport:        true,
	},
	{
		name:           "Interval",
		oid:            pgtype.IntervalOID,
		pgTypeName:     "interval",
		schemaTypeName: systemcatalog.INT64,
		value:          "interval '12h'",
		insertPlain:    true,
		valueOverride:  int64(43200000000),
		expected:       quickCheckValue[int64],
	},
	{
		name:           "Interval Array",
		oid:            pgtype.IntervalArrayOID,
		pgTypeName:     "interval[]",
		schemaTypeName: systemcatalog.INT64,
		value:          "interval '12h'",
		insertPlain:    true,
		valueOverride:  int64(12),
		expected:       quickCheckValue[int64],
		missingSupport: true,
	},
	{
		name:           "Numeric Array",
		oid:            pgtype.NumericArrayOID,
		pgTypeName:     "numeric[]",
		schemaTypeName: systemcatalog.INT64,
		value:          "interval '12h'",
		valueOverride:  int64(12),
		expected:       quickCheckValue[int64],
		missingSupport: true,
	},
	{
		name:           "Bit",
		oid:            pgtype.BitOID,
		pgTypeName:     "bit",
		schemaTypeName: systemcatalog.BOOLEAN,
		value:          "B'1'",
		insertPlain:    true,
		valueOverride:  true,
		expected:       quickCheckValue[bool],
		missingSupport: true,
	},
	{
		name:               "Bit Array",
		oid:                pgtype.BitArrayOID,
		pgTypeName:         "bit(3)",
		columnNameOverride: "bits",
		schemaTypeName:     systemcatalog.BYTES,
		value:              "B'101'",
		insertPlain:        true,
		valueOverride:      []byte{0x05},
		expected:           quickCheckValue[[]byte],
		missingSupport:     true,
	},
	{
		name:           "varbit",
		oid:            pgtype.VarbitOID,
		pgTypeName:     "bit varying",
		schemaTypeName: systemcatalog.BYTES,
		value:          []bool{true, false, true},
		valueOverride:  true,
		expected:       quickCheckValue[[]byte],
		missingSupport: true,
	},
	{
		name:           "varbit Array",
		oid:            pgtype.VarbitArrayOID,
		pgTypeName:     "bit varying[]",
		schemaTypeName: systemcatalog.BYTES,
		value:          []bool{true, false, true},
		valueOverride:  true,
		expected:       quickCheckValue[[]byte],
		missingSupport: true,
	},
	{
		name:           "Numeric",
		oid:            pgtype.NumericOID,
		pgTypeName:     "numeric",
		schemaTypeName: systemcatalog.BYTES,
		value:          "",
		valueOverride:  true,
		expected:       quickCheckValue[[]byte],
		missingSupport: true,
	},
	{
		name:           "UUID",
		oid:            pgtype.UUIDOID,
		pgTypeName:     "uuid",
		schemaTypeName: systemcatalog.STRING,
		value:          "f6df43de-36ff-40a5-9d81-caf6a79eb3f8",
		expected:       quickCheckValue[string],
	},
	{
		name:                  "UUID Array",
		oid:                   pgtype.UUIDArrayOID,
		pgTypeName:            "uuid[]",
		schemaTypeName:        systemcatalog.ARRAY,
		elementSchemaTypeName: systemcatalog.STRING,
		value:                 "'{\"f6df43de-36ff-40a5-9d81-caf6a79eb3f8\",\"9151519c-a9c9-4550-9e14-3b9860b5edff\"}'::uuid[]",
		insertPlain:           true,
		expected:              quickCheckValue[[]string],
		missingSupport:        true,
	},
	{
		name:           "JSONB",
		oid:            pgtype.JSONBOID,
		pgTypeName:     "jsonb",
		schemaTypeName: systemcatalog.STRING,
		value:          `{"foo":"bar"}`,
		expected:       quickCheckValue[string],
	},
	{
		name:           "JSONB Array",
		oid:            pgtype.JSONBArrayOID,
		pgTypeName:     "jsonb[]",
		schemaTypeName: systemcatalog.ARRAY,
		value:          `{"foo":"bar"}`,
		expected:       quickCheckValue[string],
		missingSupport: true,
	},
}

type DataTypeTestSuite struct {
	testrunner.TestRunner
}

func TestDataTypeTestSuite(t *testing.T) {
	suite.Run(t, new(DataTypeTestSuite))
}

func (dtt *DataTypeTestSuite) Test_DataType_Support() {
	for _, testCase := range dataTypeTable {
		dtt.Run(testCase.name, func() {
			if testCase.missingSupport {
				dtt.T().Skipf("Datatype %s unsupported", testCase.pgTypeName)
			}

			dtt.runDataTypeTest(testCase)
		})
	}
}

func (dtt *DataTypeTestSuite) runDataTypeTest(testCase DataTypeTest) {
	columnName := makeColumnName(testCase)

	waiter := supporting.NewWaiterWithTimeout(time.Second * 10)
	testSink := inttest.NewEventCollectorSink(
		inttest.WithFilter(
			func(_ time.Time, _ string, envelope inttest.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		inttest.WithPostHook(func(sink *inttest.EventCollectorSink) {
			if sink.NumOfEvents() == 1 {
				waiter.Signal()
			}
		}),
	)

	var tableName string
	dtt.RunTest(
		func(context testrunner.Context) error {
			if testCase.insertPlain {
				if _, err := context.Exec(stdctx.Background(),
					fmt.Sprintf(
						"INSERT INTO \"%s\" VALUES ('2023-01-01 00:00:00', %s)",
						tableName, testCase.value,
					),
				); err != nil {
					return err
				}
			} else {
				if _, err := context.Exec(stdctx.Background(),
					fmt.Sprintf("INSERT INTO \"%s\" VALUES ('2023-01-01 00:00:00', $1)", tableName),
					testCase.value,
				); err != nil {
					return err
				}
			}

			if err := waiter.Await(); err != nil {
				return err
			}

			events := testSink.Events()
			assert.Equal(dtt.T(), 1, len(events))

			event := events[0]

			// Check column schema
			schema, present := inttest.GetField("after", event.Envelope.Schema.Fields)
			assert.True(dtt.T(), present)
			assert.NotNil(dtt.T(), schema)
			columnSchema, present := inttest.GetField(columnName, schema.Fields)
			assert.True(dtt.T(), present)
			assert.NotNil(dtt.T(), columnSchema)
			assert.Equal(dtt.T(), testCase.schemaTypeName, columnSchema.Type)

			payload, present := event.Envelope.Payload.After[columnName]
			assert.True(dtt.T(), present)
			assert.NotNil(dtt.T(), payload)
			testCase.expected(dtt.T(), testCase, payload)
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			_, err := setupContext.Exec(stdctx.Background(), "CREATE EXTENSION IF NOT EXISTS ltree")
			if err != nil {
				return err
			}

			_, tableName, err = setupContext.CreateHypertable("ts", time.Hour*24,
				systemcatalog.NewColumn("ts", pgtype.TimestamptzOID, "timestamptz", false, nil),
				systemcatalog.NewColumn(columnName, testCase.oid, testCase.pgTypeName, false, nil),
			)
			if err != nil {
				return err
			}

			setupContext.AddSystemConfigConfigurator(testSink.SystemConfigConfigurator)
			return nil
		}),
	)
}

type DataTypeTest struct {
	name                  string
	oid                   uint32
	pgTypeName            string
	columnNameOverride    string
	schemaTypeName        systemcatalog.DataType
	elementSchemaTypeName systemcatalog.DataType
	value                 any
	insertPlain           bool
	valueOverride         any
	expected              func(t *testing.T, test DataTypeTest, value any)
	missingSupport        bool
}

func checkByteArray(t *testing.T, testCase DataTypeTest, value any) {
	// Byte arrays are provided as base64 encoded strings
	v := checkType[string](t, value)
	encoded := base64.StdEncoding.EncodeToString(expectedValue(testCase).([]byte))
	checkValue[string](t, encoded, v)
}

func checkChar(t *testing.T, testCase DataTypeTest, value any) {
	v := checkType[string](t, value)
	checkValue[string](t, string(expectedValue(testCase).(int32)), v)
}

func quickCheckValue[T any](t *testing.T, testCase DataTypeTest, value any) {
	v := checkType[T](t, value)
	checkValue[T](t, expectedValue(testCase).(T), v)
}

func checkValue[T any](t *testing.T, expected, value T) {
	assert.Equal(t, expected, value)
}

func checkType[T any](t *testing.T, value any) T {
	// Necessary adjustments due to JSON numbers only being float64
	rType := reflect.TypeOf(*new(T))
	switch rType.Kind() {
	case reflect.Int16:
		value = int16(value.(float64))
	case reflect.Int32:
		value = int32(value.(float64))
	case reflect.Int64:
		value = int64(value.(float64))
	case reflect.Float32:
		value = float32(value.(float64))
	}
	v, ok := value.(T)
	if !ok {
		t.Errorf("value is of type %s but was expected to be %s", reflect.TypeOf(value), rType)
	}
	return v
}

func expectedValue(testCase DataTypeTest) any {
	if testCase.valueOverride != nil {
		return testCase.valueOverride
	}
	return testCase.value
}

func makeColumnName(testCase DataTypeTest) string {
	name := testCase.pgTypeName
	if testCase.columnNameOverride != "" {
		name = testCase.columnNameOverride
	}

	name = strings.ReplaceAll(name, "[]", "_array")
	return fmt.Sprintf("val_%s", name)
}
