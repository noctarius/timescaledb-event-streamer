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

package typemanager

import (
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/samber/lo"
)

// This map is just for easier definition, and gets optimized
// at startup into a simplified array for faster index access
var coreTypeMap = map[uint32]typeRegistration{
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
	pgtypes.QCharArrayOID: {
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
	pgtypes.MacAddr8OID: {
		schemaType: schema.STRING,
		converter:  macaddr2text,
		typeFactory: func(_ *pgtype.Map, typ pgtypes.PgType) *pgtype.Type {
			return &pgtype.Type{Name: "macaddr8", OID: pgtypes.MacAddr8OID, Codec: pgtype.MacaddrCodec{}}
		},
	},
	pgtypes.MacAddrArray8OID: {
		schemaType: schema.ARRAY,
		oidElement: pgtypes.MacAddr8OID,
		converter:  arrayConverter[[]string](pgtypes.MacAddr8OID, macaddr2text),
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
	pgtypes.TimeTZOID: {
		schemaType: schema.STRING,
		converter:  time2text,
		typeFactory: func(_ *pgtype.Map, typ pgtypes.PgType) *pgtype.Type {
			return &pgtype.Type{Name: "timetz", OID: pgtypes.TimeTZOID, Codec: &pgtypes.TimetzCodec{}}
		},
	},
	pgtypes.TimeTZArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtypes.TimeTZOID,
		converter:  arrayConverter[[]string](pgtypes.TimeTZOID, time2text),
	},
	pgtypes.XmlOID: {
		schemaType: schema.STRING,
		typeFactory: func(_ *pgtype.Map, typ pgtypes.PgType) *pgtype.Type {
			return &pgtype.Type{Name: "xml", OID: pgtypes.XmlOID, Codec: pgtypes.XmlCodec{}}
		},
	},
	pgtypes.XmlArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtypes.XmlOID,
	},
	pgtype.PointOID: {
		schemaType: schema.STRING,
		converter:  point2string,
	},
	pgtype.PointArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.PointOID,
		converter:  arrayConverter[[]string](pgtype.PointOID, point2string),
	},
	pgtype.BoxOID: {
		schemaType: schema.STRING,
		converter:  box2string,
	},
	pgtype.BoxArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.BoxOID,
		converter:  arrayConverter[[]string](pgtype.BoxOID, box2string),
		codecFactory: func(typeMap *pgtype.Map) pgtype.Codec {
			if pt, present := typeMap.TypeForOID(pgtype.BoxOID); present {
				return &pgtypes.BoxArrayCodec{
					PgxArrayCodec: &pgtype.ArrayCodec{ElementType: pt},
				}
			}
			return nil
		},
		overrideExistingCodec: true,
	},
	pgtype.LineOID: {
		schemaType: schema.STRING,
		converter:  line2string,
	},
	pgtype.LineArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.LineOID,
		converter:  arrayConverter[[]string](pgtype.LineOID, line2string),
	},
	pgtype.PolygonOID: {
		schemaType: schema.STRING,
		converter:  polygon2string,
	},
	pgtype.PolygonArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.PolygonOID,
		converter:  arrayConverter[[]string](pgtype.PolygonOID, polygon2string),
	},
	pgtype.CircleOID: {
		schemaType: schema.STRING,
		converter:  circle2string,
	},
	pgtype.CircleArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.CircleOID,
		converter:  arrayConverter[[]string](pgtype.CircleOID, circle2string),
	},
	pgtype.PathOID: {
		schemaType: schema.STRING,
		converter:  path2string,
	},
	pgtype.PathArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.PathOID,
		converter:  arrayConverter[[]string](pgtype.PathArrayOID, path2string),
	},
	pgtype.LsegOID: {
		schemaType: schema.STRING,
		converter:  lseg2string,
	},
	pgtype.LsegArrayOID: {
		schemaType: schema.ARRAY,
		oidElement: pgtype.LsegOID,
		converter:  arrayConverter[[]string](pgtype.LsegArrayOID, lseg2string),
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
		codec:         pgtypes.LtreeCodec{},
	},
	"_ltree": {
		schemaType: schema.ARRAY,
		isArray:    true,
	},
	"hstore": {
		schemaType:    schema.MAP,
		schemaBuilder: schema.HStore(),
		converter:     hstore2map,
		codec:         pgtype.HstoreCodec{},
	},
	"_hstore": {
		schemaType: schema.ARRAY,
		isArray:    true,
	},
}

var upperCoreOidBound uint32
var emptyTypeRegistration = typeRegistration{}
var coreTypes []*typeRegistration

func init() {
	upperCoreOidBound = lo.Max(lo.Keys(coreTypeMap)) + 1
	coreTypes = make([]*typeRegistration, upperCoreOidBound)
	for oid, registration := range coreTypeMap {
		coreTypes[oid] = lo.ToPtr(registration)
	}
}

func coreType(
	oid uint32,
) (typeRegistration, bool) {

	if oid >= upperCoreOidBound {
		return emptyTypeRegistration, false
	}
	registration := coreTypes[oid]
	if registration == nil {
		return emptyTypeRegistration, false
	}
	return *registration, true
}
