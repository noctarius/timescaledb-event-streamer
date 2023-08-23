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

package tests

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/waiting"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"reflect"
	"strings"
	"testing"
	"time"
)

var dataTypeTable = []DataTypeTest{
	{
		name:       "Boolean",
		oid:        pgtype.BoolOID,
		pgTypeName: "boolean",
		schemaType: schema.BOOLEAN,
		value:      true,
		expected:   quickCheckValue[bool],
	},
	{
		name:              "Boolean Array",
		oid:               pgtype.BoolArrayOID,
		pgTypeName:        "boolean[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.BOOLEAN,
		value:             []bool{true, false, true},
		expected:          quickCheckValue[[]bool],
	},
	{
		name:                  "Byte Array (bytea)",
		oid:                   pgtype.ByteaOID,
		pgTypeName:            "bytea",
		schemaType:            schema.STRING,
		value:                 []byte{0xDE, 0xAD, 0xBE, 0xEF},
		expectedValueOverride: "deadbeef",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Byte Array (bytea) Array",
		oid:                   pgtype.ByteaArrayOID,
		pgTypeName:            "bytea[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 [][]byte{{0xDE, 0xAD, 0xBE, 0xEF}, {0xCA, 0xFE, 0xBA, 0xBE}},
		expectedValueOverride: []string{"deadbeef", "cafebabe"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:       "Name",
		oid:        pgtype.NameOID,
		pgTypeName: "name",
		schemaType: schema.STRING,
		value:      "testname",
		expected:   quickCheckValue[string],
	},
	{
		name:              "Name Array",
		oid:               pgtype.NameArrayOID,
		pgTypeName:        "name[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"F", "T", "O"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:       "Int (16bit)",
		oid:        pgtype.Int2OID,
		pgTypeName: "int2",
		schemaType: schema.INT16,
		value:      int16(16),
		expected:   quickCheckValue[int16],
	},
	{
		name:              "Int (16bit) Array",
		oid:               pgtype.Int2ArrayOID,
		pgTypeName:        "int2[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.INT16,
		value:             []int16{5, 10, 15, -32768, 32767},
		expected:          quickCheckValue[[]int16],
	},
	{
		name:       "Int (32bit)",
		oid:        pgtype.Int4OID,
		pgTypeName: "int4",
		schemaType: schema.INT32,
		value:      int32(32),
		expected:   quickCheckValue[int32],
	},
	{
		name:              "Int (32bit) Array",
		oid:               pgtype.Int4ArrayOID,
		pgTypeName:        "int4[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.INT32,
		value:             []int32{5, 10, 15, -2147483648, 2147483647},
		expected:          quickCheckValue[[]int32],
	},
	{
		name:       "Int (64bit)",
		oid:        pgtype.Int8OID,
		pgTypeName: "int8",
		schemaType: schema.INT64,
		value:      int64(64),
		expected:   quickCheckValue[int64],
	},
	{
		name:              "Int (64bit) Array",
		oid:               pgtype.Int8ArrayOID,
		pgTypeName:        "int8[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.INT64,
		value:             []int64{5, 10, 15, -9223372036854775808},
		expected:          quickCheckValue[[]int64],
	},
	{
		name:       "Text",
		oid:        pgtype.TextOID,
		pgTypeName: "text",
		schemaType: schema.STRING,
		value:      "Some Test Text",
		expected:   quickCheckValue[string],
	},
	{
		name:              "Text Array",
		oid:               pgtype.TextArrayOID,
		pgTypeName:        "text[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"first", "second", "third"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:       "JSON",
		oid:        pgtype.JSONOID,
		pgTypeName: "json",
		schemaType: schema.STRING,
		value:      `{"foo":"bar"}`,
		expected:   quickCheckValue[string],
	},
	{
		name:                  "JSON Array",
		oid:                   pgtype.JSONArrayOID,
		pgTypeName:            "json[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 `array['{"foo":"bar"}','{"bar":"foo"}']::json[]`,
		insertPlain:           true,
		expectedValueOverride: []string{`{"foo":"bar"}`, `{"bar":"foo"}`},
		expected:              quickCheckValue[[]string],
	},
	{
		name:       "CIDR (IPv4)",
		oid:        pgtype.CIDROID,
		pgTypeName: "cidr",
		schemaType: schema.STRING,
		value:      `10.0.0.0/24`,
		expected:   quickCheckValue[string],
	},
	{
		name:              "CIDR (IPv4) Array",
		oid:               pgtype.CIDRArrayOID,
		pgTypeName:        "cidr[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{`10.0.0.0/24`, `192.168.0.0/16`},
		expected:          quickCheckValue[[]string],
	},
	{
		name:       "CIDR (IPv6)",
		oid:        pgtype.CIDROID,
		pgTypeName: "cidr",
		schemaType: schema.STRING,
		value:      `2001:4f8:3:ba::/64`,
		expected:   quickCheckValue[string],
	},
	{
		name:              "CIDR (IPv6) Array",
		oid:               pgtype.CIDRArrayOID,
		pgTypeName:        "cidr[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{`2001:4f8:3:ba::/64`, `2001:4f8:3:ba::/90`},
		expected:          quickCheckValue[[]string],
	},
	{
		name:       "Float (32bit)",
		oid:        pgtype.Float4OID,
		pgTypeName: "float4",
		schemaType: schema.FLOAT32,
		value:      float32(13.1),
		expected:   quickCheckValue[float32],
	},
	{
		name:              "Float (32bit) Array",
		oid:               pgtype.Float4ArrayOID,
		pgTypeName:        "float4[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.FLOAT32,
		value:             []float32{14.1, 12.7},
		expected:          quickCheckValue[[]float32],
	},
	{
		name:       "Float (64bit)",
		oid:        pgtype.Float8OID,
		pgTypeName: "float8",
		schemaType: schema.FLOAT64,
		value:      13.1,
		expected:   quickCheckValue[float64],
	},
	{
		name:              "Float (64bit) Array",
		oid:               pgtype.Float8ArrayOID,
		pgTypeName:        "float8[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.FLOAT64,
		value:             []float64{14.1, 12.7},
		expected:          quickCheckValue[[]float64],
	},
	{
		name:                  "MAC Address",
		oid:                   pgtype.MacaddrOID,
		pgTypeName:            "macaddr",
		schemaType:            schema.STRING,
		value:                 "08:00:2B:01:02:03",
		expectedValueOverride: "08:00:2b:01:02:03",
		expected:              quickCheckValue[string],
	},
	{
		name:              "MAC Address Array",
		oid:               pgtype.MacaddrOID,
		pgTypeName:        "macaddr[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"08:00:2b:01:02:03", "01:02:03:04:05:06"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:                  "MAC Address (EUI-64)",
		oid:                   pgtypes.MacAddr8OID,
		pgTypeName:            "macaddr8",
		schemaType:            schema.STRING,
		value:                 "08:00:2B:01:02:03:04:05",
		expectedValueOverride: "08:00:2b:01:02:03:04:05",
		expected:              quickCheckValue[string],
	},
	{
		name:              "MAC Address (EUI-64) Array",
		oid:               pgtypes.MacAddrArray8OID,
		pgTypeName:        "macaddr8[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"08:00:2b:01:02:03:04:05", "01:02:03:04:05:06:07:08"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:       "Inet (IPv4)",
		oid:        pgtype.InetOID,
		pgTypeName: "inet",
		schemaType: schema.STRING,
		value:      "127.0.0.1/32",
		expected:   quickCheckValue[string],
	},
	{
		name:              "Inet (IPv4) Array",
		oid:               pgtype.InetArrayOID,
		pgTypeName:        "inet[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"127.0.0.1/32", "192.168.100.1/24"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:       "Inet (IPv6)",
		oid:        pgtype.InetOID,
		pgTypeName: "inet",
		schemaType: schema.STRING,
		value:      "::1/128",
		expected:   quickCheckValue[string],
	},
	{
		name:              "Inet (IPv6) Array",
		oid:               pgtype.InetArrayOID,
		pgTypeName:        "inet[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		//value:             "'{\"::1/128\", \"2000::1/64\"}'::inet[]",
		value:    []string{"::1/128", "2000::1/64"},
		expected: quickCheckValue[[]string],
	},
	{
		name:                  "Date",
		oid:                   pgtype.DateOID,
		pgTypeName:            "date",
		schemaType:            schema.INT32,
		value:                 "2023-01-01",
		expectedValueOverride: int32(19358),
		expected:              quickCheckValue[int32],
	},
	{
		name:                  "Date Array",
		oid:                   pgtype.DateArrayOID,
		pgTypeName:            "date[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.INT32,
		value:                 []string{"1984-01-24", "1900-01-01"},
		expectedValueOverride: []int32{5136, -25567},
		expected:              quickCheckValue[[]int32],
	},
	{
		name:       "Time Without Timezone",
		oid:        pgtype.TimeOID,
		pgTypeName: "time",
		schemaType: schema.STRING,
		value:      "12:00:12.054321",
		expected:   quickCheckValue[string],
	},
	{
		name:              "Time Without Timezone Array",
		oid:               pgtype.TimeArrayOID,
		pgTypeName:        "time[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"12:00:12.054321", "14:00:14.054000"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:                  "Time With Timezone",
		oid:                   pgtypes.TimeTZOID,
		pgTypeName:            "timetz",
		schemaType:            schema.STRING,
		value:                 "12:00:12.054321Z07:30",
		expectedValueOverride: "19:30:12.054321",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Time With Timezone Array",
		oid:                   pgtypes.TimeTZArrayOID,
		pgTypeName:            "timetz[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 []string{"12:00:12.054321Z07:00", "14:00:14.054000Z00:30"},
		expectedValueOverride: []string{"19:00:12.054321", "14:30:14.054000"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Timestamp Without Timezone",
		oid:                   pgtype.TimestampOID,
		pgTypeName:            "timestamp",
		schemaType:            schema.INT64,
		value:                 "2023-01-01T12:00:12.054321",
		expectedValueOverride: int64(1672574412054),
		expected:              quickCheckValue[int64],
	},
	{
		name:                  "Timestamp Without Timezone Array",
		oid:                   pgtype.TimestampArrayOID,
		pgTypeName:            "timestamp[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.INT64,
		value:                 []string{"2023-01-01T12:00:12.054321", "2022-01-01T12:00:12.054321"},
		expectedValueOverride: []int64{1672574412054, 1641038412054},
		expected:              quickCheckValue[[]int64],
	},
	{
		name:                  "Timestamp With Timezone",
		oid:                   pgtype.TimestamptzOID,
		pgTypeName:            "timestamptz",
		schemaType:            schema.STRING,
		value:                 "2023-01-01T12:00:12.054321Z07:00",
		expectedValueOverride: "2023-01-01T19:00:12.054321Z",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Timestamp With Timezone Array",
		oid:                   pgtype.TimestamptzArrayOID,
		pgTypeName:            "timestamptz[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 []string{"2023-01-01T12:00:12.054321Z07:00", "2027-03-01T12:01:12.000000Z03:00"},
		expectedValueOverride: []string{"2023-01-01T19:00:12.054321Z", "2027-03-01T15:01:12Z"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Interval",
		oid:                   pgtype.IntervalOID,
		pgTypeName:            "interval",
		schemaType:            schema.INT64,
		value:                 "interval '12h'",
		insertPlain:           true,
		expectedValueOverride: int64(43200000000),
		expected:              quickCheckValue[int64],
	},
	{
		name:                  "Interval Array",
		oid:                   pgtype.IntervalArrayOID,
		pgTypeName:            "interval[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.INT64,
		value:                 "array[interval '12h', interval '6d', interval '2mon']::interval[]",
		insertPlain:           true,
		expectedValueOverride: []int64{43200000000, 518400000000, 5259600000000},
		expected:              quickCheckValue[[]int64],
	},
	{
		name:       "UUID",
		oid:        pgtype.UUIDOID,
		pgTypeName: "uuid",
		schemaType: schema.STRING,
		value:      "f6df43de-36ff-40a5-9d81-caf6a79eb3f8",
		expected:   quickCheckValue[string],
	},
	{
		name:                  "UUID Array",
		oid:                   pgtype.UUIDArrayOID,
		pgTypeName:            "uuid[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"f6df43de-36ff-40a5-9d81-caf6a79eb3f8\",\"9151519c-a9c9-4550-9e14-3b9860b5edff\"}'::uuid[]",
		insertPlain:           true,
		expectedValueOverride: []string{"f6df43de-36ff-40a5-9d81-caf6a79eb3f8", "9151519c-a9c9-4550-9e14-3b9860b5edff"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:       "JSONB",
		oid:        pgtype.JSONBOID,
		pgTypeName: "jsonb",
		schemaType: schema.STRING,
		value:      `{"foo":"bar"}`,
		expected:   quickCheckValue[string],
	},
	{
		name:                  "JSONB Array",
		oid:                   pgtype.JSONBArrayOID,
		pgTypeName:            "jsonb[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 `array['{"foo":"bar"}','{"bar":"foo"}']::jsonb[]`,
		insertPlain:           true,
		expectedValueOverride: []string{`{"foo":"bar"}`, `{"bar":"foo"}`},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Int4Range",
		oid:                   pgtype.Int4rangeOID,
		pgTypeName:            "int4range",
		schemaType:            schema.STRING,
		value:                 "'(10,20)'::int4range",
		insertPlain:           true,
		expectedValueOverride: "[11,20)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Int4Range Array",
		oid:                   pgtype.Int4rangeArrayOID,
		pgTypeName:            "int4range[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"(10,20)\",\"(10,20]\",\"(,20)\",\"(10,)\"}'::int4range[]",
		insertPlain:           true,
		expectedValueOverride: []string{"[11,20)", "[11,21)", "(,20)", "[11,)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Int8Range",
		oid:                   pgtype.Int8rangeOID,
		pgTypeName:            "int8range",
		schemaType:            schema.STRING,
		value:                 "'(10,200000)'::int8range",
		insertPlain:           true,
		expectedValueOverride: "[11,200000)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Int8Range Array",
		oid:                   pgtype.Int8rangeArrayOID,
		pgTypeName:            "int8range[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"(10,200000)\",\"(10,200000]\",\"(,200000)\",\"(10,)\"}'::int8range[]",
		insertPlain:           true,
		expectedValueOverride: []string{"[11,200000)", "[11,200001)", "(,200000)", "[11,)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Numeric Range",
		oid:                   pgtype.NumrangeOID,
		pgTypeName:            "numrange",
		schemaType:            schema.STRING,
		value:                 "'(10.1,200000.2)'::numrange",
		insertPlain:           true,
		expectedValueOverride: "(10.1,200000.2)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Numeric Range Array",
		oid:                   pgtype.NumrangeArrayOID,
		pgTypeName:            "numrange[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"(10.1,200000.2)\",\"(10.1,200000.2]\",\"(,200000.2)\",\"(10.1,)\"}'::numrange[]",
		insertPlain:           true,
		expectedValueOverride: []string{"(10.1,200000.2)", "(10.1,200000.2]", "(,200000.2)", "(10.1,)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Timestamp Without Timezone Range",
		oid:                   pgtype.TsrangeOID,
		pgTypeName:            "tsrange",
		schemaType:            schema.STRING,
		value:                 "'(\"2022-01-01T12:00:12.054321\", \"2023-01-01T12:00:12.054321\")'::tsrange",
		insertPlain:           true,
		expectedValueOverride: "(2022-01-01T12:00:12.054321,2023-01-01T12:00:12.054321)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Timestamp Without Timezone Range Array",
		oid:                   pgtype.TsrangeArrayOID,
		pgTypeName:            "tsrange[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "array['(\"2022-01-01T12:00:12.054321\",\"2023-01-01T12:00:12.054321\")','(\"1984-01-01T00:00:00\",\"1984-01-24T12:00:00\")']::tsrange[]",
		insertPlain:           true,
		expectedValueOverride: []string{"(2022-01-01T12:00:12.054321,2023-01-01T12:00:12.054321)", "(1984-01-01T00:00:00,1984-01-24T12:00:00)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Timestamp With Timezone Range",
		oid:                   pgtype.TstzrangeOID,
		pgTypeName:            "tstzrange",
		schemaType:            schema.STRING,
		value:                 "'(\"2022-01-01T12:00:12.054321\", \"2023-01-01T12:00:12.054321\")'::tstzrange",
		insertPlain:           true,
		expectedValueOverride: "(2022-01-01T12:00:12.054321Z,2023-01-01T12:00:12.054321Z)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Timestamp With Timezone Range Array",
		oid:                   pgtype.TstzrangeArrayOID,
		pgTypeName:            "tstzrange[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "array['(\"2022-01-01T12:00:12.054321+07:00\",\"2023-01-01T12:00:12.054321\")','(\"1984-01-01T00:00:00\",\"1984-01-24T12:00:00\")']::tstzrange[]",
		insertPlain:           true,
		expectedValueOverride: []string{"(2022-01-01T05:00:12.054321Z,2023-01-01T12:00:12.054321Z)", "(1984-01-01T00:00:00Z,1984-01-24T12:00:00Z)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Date Range",
		oid:                   pgtype.DaterangeOID,
		pgTypeName:            "daterange",
		schemaType:            schema.STRING,
		value:                 "'(\"2022-01-01\", \"2023-01-01\")'::daterange",
		insertPlain:           true,
		expectedValueOverride: "[2022-01-02,2023-01-01)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Date Range Array",
		oid:                   pgtype.DaterangeArrayOID,
		pgTypeName:            "daterange[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "array['(\"2022-01-01\",\"2023-01-01\")','[\"1984-01-01\",\"1984-01-24\")']::daterange[]",
		insertPlain:           true,
		expectedValueOverride: []string{"[2022-01-02,2023-01-01)", "[1984-01-01,1984-01-24)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Quoted Char",
		oid:                   pgtype.QCharOID,
		pgTypeName:            "\"char\"",
		columnNameOverride:    "qchar",
		schemaType:            schema.STRING,
		value:                 'F',
		expectedValueOverride: "F",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Quoted Char Array",
		oid:                   pgtype.QCharArrayOID,
		pgTypeName:            "\"char\"[]",
		columnNameOverride:    "qchar[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"F\",\"T\",\"O\"}'::\"char\"[]",
		insertPlain:           true,
		expectedValueOverride: []string{"F", "T", "O"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:       "OID",
		oid:        pgtype.OIDOID,
		pgTypeName: "oid",
		schemaType: schema.INT64,
		value:      int64(123),
		expected:   quickCheckValue[int64],
	},
	{
		name:              "OID Array",
		oid:               pgtype.OIDArrayOID,
		pgTypeName:        "oid[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.INT64,
		value:             []int64{10, 11, 12},
		expected:          quickCheckValue[[]int64],
	},
	{
		name:       "XID",
		oid:        pgtype.XIDOID,
		pgTypeName: "xid",
		schemaType: schema.INT64,
		value:      int64(123),
		expected:   quickCheckValue[int64],
	},
	{
		name:              "XID Array",
		oid:               pgtype.XIDArrayOID,
		pgTypeName:        "xid[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.INT64,
		value:             []int64{10, 11, 12},
		expected:          quickCheckValue[[]int64],
	},
	{
		name:       "CID",
		oid:        pgtype.CIDOID,
		pgTypeName: "cid",
		schemaType: schema.INT64,
		value:      int64(123),
		expected:   quickCheckValue[int64],
	},
	{
		name:              "CID Array",
		oid:               pgtype.XIDArrayOID,
		pgTypeName:        "cid[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.INT64,
		value:             []int64{10, 11, 12},
		expected:          quickCheckValue[[]int64],
	},
	{
		name:       "LSEG",
		oid:        pgtype.LsegOID,
		pgTypeName: "lseg",
		schemaType: schema.STRING,
		value:      "[(1,2),(2,1)]",
		expected:   quickCheckValue[string],
	},
	{
		name:                  "LSEG Array",
		oid:                   pgtype.LsegArrayOID,
		pgTypeName:            "lseg[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['[(1,2),(2,1)]', '[(10,20),(20,10)]']::lseg[]",
		insertPlain:           true,
		expectedValueOverride: []string{"[(1,2),(2,1)]", "[(10,20),(20,10)]"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Path",
		oid:                   pgtype.PathOID,
		pgTypeName:            "path",
		schemaType:            schema.STRING,
		value:                 `'(-10,-20),(0,0),(10,20)'::path`,
		insertPlain:           true,
		expectedValueOverride: "((-10,-20),(0,0),(10,20))",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Path Array",
		oid:                   pgtype.PathArrayOID,
		pgTypeName:            "path[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['((-10,-20),(0,0),(10,20))', '[(0,0),(100,100),(50,50),(0,0)]']::path[]",
		insertPlain:           true,
		expectedValueOverride: []string{"((-10,-20),(0,0),(10,20))", "[(0,0),(100,100),(50,50),(0,0)]"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Box",
		oid:                   pgtype.BoxOID,
		pgTypeName:            "box",
		schemaType:            schema.STRING,
		value:                 "'(10,20),(30,40)'",
		insertPlain:           true,
		expectedValueOverride: "(30,40),(10,20)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Box Array",
		oid:                   pgtype.BoxArrayOID,
		pgTypeName:            "box[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['(10,20),(30,40)','(-10,-20),(0,0)']::box[]",
		insertPlain:           true,
		expectedValueOverride: []string{"(30,40),(10,20)", "(0,0),(-10,-20)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Point",
		oid:                   pgtype.PointOID,
		pgTypeName:            "point",
		schemaType:            schema.STRING,
		value:                 "'(10,20)'",
		insertPlain:           true,
		expectedValueOverride: "(10,20)",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Point Array",
		oid:                   pgtype.PointArrayOID,
		pgTypeName:            "point[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['(10,20)','(0,0)']::point[]",
		insertPlain:           true,
		expectedValueOverride: []string{"(10,20)", "(0,0)"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Polygon",
		oid:                   pgtype.PolygonOID,
		pgTypeName:            "polygon",
		schemaType:            schema.STRING,
		value:                 `'((-10,-20),(0,0),(10,20))'::polygon`,
		insertPlain:           true,
		expectedValueOverride: "((-10,-20),(0,0),(10,20))",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Polygon Array",
		oid:                   pgtype.PolygonArrayOID,
		pgTypeName:            "polygon[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['((-10,-20),(0,0),(10,20))', '((0,0),(100,100),(50,50),(0,0))']::polygon[]",
		insertPlain:           true,
		expectedValueOverride: []string{"((-10,-20),(0,0),(10,20))", "((0,0),(100,100),(50,50),(0,0))"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:       "Line",
		oid:        pgtype.LineOID,
		pgTypeName: "line",
		schemaType: schema.STRING,
		value:      "{0,-1,0}",
		expected:   quickCheckValue[string],
	},
	{
		name:                  "Line Array",
		oid:                   pgtype.LineArrayOID,
		pgTypeName:            "line[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['{0,-1,0}','{100,-1,100}']::line[]",
		insertPlain:           true,
		expectedValueOverride: []string{"{0,-1,0}", "{100,-1,100}"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:       "Circle",
		oid:        pgtype.CircleOID,
		pgTypeName: "circle",
		schemaType: schema.STRING,
		value:      "<(0,0),1>",
		expected:   quickCheckValue[string],
	},
	{
		name:                  "Circle Array",
		oid:                   pgtype.CircleArrayOID,
		pgTypeName:            "circle[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "ARRAY['<(0,0),1>','<(4.0,20.0),30>']::circle[]",
		insertPlain:           true,
		expectedValueOverride: []string{"<(0,0),1>", "<(4,20),30>"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:               "Fixed Length Char",
		oid:                pgtype.BPCharOID,
		pgTypeName:         "char(3)",
		columnNameOverride: "bpchar",
		schemaType:         schema.STRING,
		value:              "  F",
		expected:           quickCheckValue[string],
	},
	{
		name:               "Fixed Length Char Array",
		oid:                pgtype.BPCharArrayOID,
		pgTypeName:         "char(4)[]",
		columnNameOverride: "bpchar_array",
		schemaType:         schema.ARRAY,
		elementSchemaType:  schema.STRING,
		value:              []string{"P  F", "1234"},
		expected:           quickCheckValue[[]string],
	},
	{
		name:       "Varchar",
		oid:        pgtype.VarcharOID,
		pgTypeName: "varchar",
		schemaType: schema.STRING,
		value:      "F",
		expected:   quickCheckValue[string],
	},
	{
		name:              "Varchar Array",
		oid:               pgtype.VarcharArrayOID,
		pgTypeName:        "varchar[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRING,
		value:             []string{"first", "second", "third"},
		expected:          quickCheckValue[[]string],
	},
	{
		name:                  "Bit",
		oid:                   pgtype.BitOID,
		pgTypeName:            "bit",
		schemaType:            schema.STRING,
		value:                 "B'1'",
		insertPlain:           true,
		expectedValueOverride: "1",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Bit Array",
		oid:                   pgtype.BitArrayOID,
		pgTypeName:            "bit(3)[]",
		columnNameOverride:    "bits",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{B101,B111}'::varbit[]",
		insertPlain:           true,
		expectedValueOverride: []string{"101", "111"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Bit Varying",
		oid:                   pgtype.VarbitOID,
		pgTypeName:            "varbit",
		schemaType:            schema.STRING,
		value:                 "B'10101'",
		insertPlain:           true,
		expectedValueOverride: "10101",
		expected:              quickCheckValue[string],
	},
	{
		name:                  "Bit Varying Array",
		oid:                   pgtype.VarbitArrayOID,
		pgTypeName:            "varbit[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{B101,B1011111111}'::varbit[]",
		insertPlain:           true,
		expectedValueOverride: []string{"101", "1011111111"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                  "Numeric",
		oid:                   pgtype.NumericOID,
		pgTypeName:            "numeric",
		schemaType:            schema.FLOAT64,
		value:                 "12.1",
		expectedValueOverride: 12.1,
		expected:              quickCheckValue[float64],
	},
	{
		name:                  "Numeric Array",
		oid:                   pgtype.NumericArrayOID,
		pgTypeName:            "numeric[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.FLOAT64,
		value:                 "'{12.1,1.23}'::numeric[]",
		insertPlain:           true,
		expectedValueOverride: []float64{12.1, 1.23},
		expected:              quickCheckValue[[]float64],
	},
	{
		name:       "Ltree",
		dynamicOid: true,
		pgTypeName: "ltree",
		schemaType: schema.STRING,
		value:      "foo.bar",
		expected:   quickCheckValue[string],
	},
	{
		name:       "Ltree Array",
		dynamicOid: true,
		pgTypeName: "ltree[]",
		schemaType: schema.ARRAY,
		value:      []string{"foo.bar", "bar.foo"},
		expected:   quickCheckValue[[]string],
	},
	{
		name:       "Xml",
		oid:        pgtypes.XmlOID,
		pgTypeName: "xml",
		schemaType: schema.STRING,
		value:      "<test><simple>foo</simple></test>",
		expected:   quickCheckValue[string],
	},
	{
		name:                  "Xml Array",
		oid:                   pgtypes.XmlArrayOID,
		pgTypeName:            "xml[]",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"<test><simple>foo</simple></test>\",\"<teeeeest></teeeeest>\"}'::xml[]",
		insertPlain:           true,
		expectedValueOverride: []string{"<test><simple>foo</simple></test>", "<teeeeest></teeeeest>"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                 "Enum Type",
		pgTypeName:           "myenum",
		customTypeDefinition: "CREATE TYPE tsdb.myenum AS ENUM ('Foo', 'Bar')",
		schemaType:           schema.STRING,
		value:                "Foo",
		expected:             quickCheckValue[string],
	},
	{
		name:                  "Enum Type Array",
		pgTypeName:            "myenumarray[]",
		customTypeDefinition:  "CREATE TYPE tsdb.myenumarray AS ENUM ('Foo', 'Bar')",
		schemaType:            schema.ARRAY,
		elementSchemaType:     schema.STRING,
		value:                 "'{\"Foo\",\"Bar\"}'::myenumarray[]",
		insertPlain:           true,
		expectedValueOverride: []string{"Foo", "Bar"},
		expected:              quickCheckValue[[]string],
	},
	{
		name:                 "Composite Type",
		pgTypeName:           "mytype",
		customTypeDefinition: "CREATE TYPE tsdb.mytype AS (id uuid, time timestamptz)",
		schemaType:           schema.STRUCT,
		value:                "'(\"392eefd4-1892-46fc-ad73-953f024bd176\", \"2023-01-01\")'::mytype",
		insertPlain:          true,
		expectedValueOverride: map[string]any{
			"id":   "392eefd4-1892-46fc-ad73-953f024bd176",
			"time": "2023-01-01T00:00:00Z",
		},
		expected: quickCheckValue[map[string]any],
	},
	{
		name:                 "Composite Type Array",
		pgTypeName:           "mytypearray[]",
		customTypeDefinition: "CREATE TYPE tsdb.mytypearray AS (id uuid, time timestamptz)",
		schemaType:           schema.ARRAY,
		elementSchemaType:    schema.STRUCT,
		value:                "ARRAY['(\"392eefd4-1892-46fc-ad73-953f024bd176\", \"2023-01-01\")','(\"80a6b4c1-b5aa-472c-b8d2-b18606ef8d68\", \"2022-02-01\")']::mytypearray[]",
		insertPlain:          true,
		expectedValueOverride: []map[string]any{
			{
				"id":   "392eefd4-1892-46fc-ad73-953f024bd176",
				"time": "2023-01-01T00:00:00Z",
			},
			{
				"id":   "80a6b4c1-b5aa-472c-b8d2-b18606ef8d68",
				"time": "2022-02-01T00:00:00Z",
			},
		},
		expected: quickCheckValue[[]map[string]any],
	},
	{
		name:        "HStore",
		pgTypeName:  "hstore",
		schemaType:  schema.MAP,
		value:       "'1=>\"value\", key2=>foo'::hstore",
		insertPlain: true,
		expectedValueOverride: map[string]any{
			"1":    "value",
			"key2": "foo",
		},
		expected: quickCheckValue[map[string]any],
	},
	{
		name:              "HStore Array",
		pgTypeName:        "hstore[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.MAP,
		value:             "ARRAY['1=>\"value\", key2=>foo','\"key3\"=>1, foo=>bar']::hstore[]",
		insertPlain:       true,
		expectedValueOverride: []map[string]any{
			{
				"1":    "value",
				"key2": "foo",
			},
			{
				"key3": "1",
				"foo":  "bar",
			},
		},
		expected: quickCheckValue[[]map[string]any],
	},
	{
		name:        "Geometry",
		pgTypeName:  "geometry",
		schemaType:  schema.STRUCT,
		value:       "'010100000000000000000024C000000000000034C0'::geometry",
		insertPlain: true,
		expectedValueOverride: map[string]any{
			"wkb":  "AAAAAAHAJAAAAAAAAMA0AAAAAAAA",
			"srid": float64(0),
		},
		expected: quickCheckValue[map[string]any],
	},
	{
		name:              "Geometry Array",
		pgTypeName:        "geometry[]",
		schemaType:        schema.ARRAY,
		elementSchemaType: schema.STRUCT,
		value:             "array['010100000000000000000024C000000000000034C0','010100000000000000000000000000000000000000']::geometry[]",
		insertPlain:       true,
		expectedValueOverride: []map[string]any{
			{
				"wkb":  "AAAAAAHAJAAAAAAAAMA0AAAAAAAA",
				"srid": float64(0),
			},
			{
				"wkb":  "AAAAAAEAAAAAAAAAAAAAAAAAAAAA",
				"srid": float64(0),
			},
		},
		expected: quickCheckValue[[]map[string]any],
	},
}

const lookupTypeOidQuery = "SELECT oid FROM pg_catalog.pg_type where typname = $1"

type DataTypeTestSuite struct {
	testrunner.TestRunner
}

func TestDataTypeTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(DataTypeTestSuite))
}

func (dtt *DataTypeTestSuite) Test_DataType_Support() {
	for _, testCase := range dataTypeTable {
		dtt.Run(testCase.name, func() {
			if testCase.missingSupport {
				dtt.T().Skipf("Datatype %s unsupported", testCase.pgTypeName)
			}

			if testCase.dynamicOid {
				dtt.runDynamicDataTypeTest(&testCase)
			} else {
				dtt.runDataTypeTest(&testCase, nil)
			}
		})
	}
}

func (dtt *DataTypeTestSuite) runDynamicDataTypeTest(
	testCase *DataTypeTest,
) {

	typeName := testCase.pgTypeName
	if strings.HasSuffix(typeName, "[]") {
		typeName = fmt.Sprintf("_%s", typeName[:len(typeName)-2])
	}

	dtt.runDataTypeTest(testCase, func(setupContext testrunner.SetupContext) error {
		if err := setupContext.QueryRow(
			context.Background(), lookupTypeOidQuery, typeName,
		).Scan(&testCase.oid); err != nil {
			return err
		}
		return nil
	})
}

func (dtt *DataTypeTestSuite) runDataTypeTest(
	testCase *DataTypeTest, setupFn func(setupContext testrunner.SetupContext) error,
) {

	columnName := makeColumnName(testCase)

	waiter := waiting.NewWaiterWithTimeout(time.Second * 10)
	testSink := testsupport.NewEventCollectorSink(
		testsupport.WithFilter(
			func(_ time.Time, _ string, envelope testsupport.Envelope) bool {
				return envelope.Payload.Op == schema.OP_CREATE
			},
		),
		testsupport.WithPostHook(func(sink *testsupport.EventCollectorSink, _ testsupport.Envelope) {
			if sink.NumOfEvents() < 3 {
				waiter.Signal()
			}
		}),
	)

	var tableName string
	dtt.RunTest(
		func(ctx testrunner.Context) error {
			insert := func(t time.Time) error {
				if testCase.insertPlain {
					if _, err := ctx.Exec(context.Background(),
						fmt.Sprintf(
							"INSERT INTO \"%s\" VALUES ($1, %s)",
							tableName, testCase.value,
						), t,
					); err != nil {
						return err
					}
				} else {
					if _, err := ctx.Exec(context.Background(),
						fmt.Sprintf("INSERT INTO \"%s\" VALUES ($1, $2)", tableName),
						t, testCase.value,
					); err != nil {
						return err
					}
				}
				return nil
			}

			if err := insert(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
				return err
			}
			if err := waiter.Await(); err != nil {
				return err
			}

			waiter.Reset()
			if err := insert(time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC)); err != nil {
				return err
			}
			if err := waiter.Await(); err != nil {
				return err
			}

			events := testSink.Events()
			assert.Equal(dtt.T(), 2, len(events))

			for _, event := range events {
				// Check column schema
				schema, present := testsupport.GetField("after", event.Envelope.Schema.Fields)
				assert.True(dtt.T(), present)
				assert.NotNil(dtt.T(), schema)
				columnSchema, present := testsupport.GetField(columnName, schema.Fields)
				assert.True(dtt.T(), present)
				assert.NotNil(dtt.T(), columnSchema)
				assert.Equal(dtt.T(), testCase.schemaType, columnSchema.Type)

				payload, present := event.Envelope.Payload.After[columnName]
				assert.True(dtt.T(), present)
				assert.NotNil(dtt.T(), payload)
				testCase.expected(dtt.T(), testCase, payload)
			}
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			if setupFn != nil {
				if err := setupFn(setupContext); err != nil {
					return err
				}
			}

			if testCase.customTypeDefinition != "" {
				if _, err := setupContext.Exec(context.Background(), testCase.customTypeDefinition); err != nil {
					return err
				}
			}

			_, tn, err := setupContext.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, false, nil),
				testsupport.NewColumn(columnName, testCase.pgTypeName, false, false, nil),
			)
			if err != nil {
				return err
			}
			tableName = tn

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
	customTypeDefinition  string
	schemaType            schema.Type
	elementSchemaType     schema.Type
	value                 any
	insertPlain           bool
	expectedValueOverride any
	dynamicOid            bool
	expected              func(t *testing.T, test *DataTypeTest, value any)
	missingSupport        bool
}

func quickCheckValue[T any](
	t *testing.T, testCase *DataTypeTest, value any,
) {

	v := checkType[T](t, value)
	checkValue[T](t, expectedValue(testCase).(T), v)
}

func checkValue[T any](
	t *testing.T, expected, value T,
) {

	assert.Equal(t, expected, value)
}

func checkType[T any](
	t *testing.T, value any,
) T {

	expectedType := reflect.TypeOf(*new(T))
	return unwrapType(t, expectedType, value).(T)
}

func unwrapType(
	t *testing.T, expectedType reflect.Type, value any,
) any {

	// Necessary adjustments due to JSON numbers only being float64
	switch expectedType.Kind() {
	case reflect.Int16:
		value = int16(value.(float64))
	case reflect.Int32:
		value = int32(value.(float64))
	case reflect.Int64:
		value = int64(value.(float64))
	case reflect.Float32:
		value = float32(value.(float64))
	case reflect.Float64:
		value = value.(float64)
	case reflect.String:
		value = value.(string)
	case reflect.Bool:
		value = value.(bool)
	case reflect.Array:
	case reflect.Slice:
		elementType := expectedType.Elem()
		sliceType := reflect.SliceOf(elementType)

		// Source reflect value
		sourceValue := reflect.ValueOf(value)
		sourceLength := sourceValue.Len()

		// Create target slice
		targetValue := reflect.MakeSlice(sliceType, sourceLength, sourceLength)
		for i := 0; i < sourceLength; i++ {
			// Retrieve index value from source
			sourceIndex := sourceValue.Index(i)

			// Unwrap the source entry
			v := unwrapType(t, elementType, sourceIndex.Interface())

			// Set in target slice
			targetValue.Index(i).Set(
				reflect.ValueOf(v).Convert(elementType),
			)
		}
		value = targetValue.Interface()
	}
	return value
}

func expectedValue(
	testCase *DataTypeTest,
) any {

	if testCase.expectedValueOverride != nil {
		return testCase.expectedValueOverride
	}
	return testCase.value
}

func makeColumnName(
	testCase *DataTypeTest,
) string {

	name := testCase.pgTypeName
	if testCase.columnNameOverride != "" {
		name = testCase.columnNameOverride
	}

	name = strings.ReplaceAll(name, "[]", "_array")
	return fmt.Sprintf("val_%s", name)
}
