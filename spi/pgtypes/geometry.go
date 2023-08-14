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

package pgtypes

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/geojson"
)

type GeometryScanner interface {
	ScanGeometry(
		v Geometry,
	) error
}

type GeometryValuer interface {
	GeometryValue() (Geometry, error)
}

type Geometry struct {
	Geometry geom.T
	Valid    bool
}

func (g *Geometry) ScanGeometry(
	v Geometry,
) error {

	*g = v
	return nil
}

func (g Geometry) GeometryValue() (Geometry, error) {
	return g, nil
}

func (g *Geometry) Scan(
	src any,
) error {

	if src == nil {
		*g = Geometry{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextLtreeToLtreeScanner{}.Scan([]byte(src), g)
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (g Geometry) Value() (driver.Value, error) {
	if !g.Valid {
		return nil, nil
	}

	return g.Geometry, nil
}

func (g Geometry) MarshalJSON() ([]byte, error) {
	if !g.Valid {
		return []byte("null"), nil
	}

	return geojson.Marshal(g.Geometry)
}

func (g *Geometry) UnmarshalJSON(
	b []byte,
) error {

	var geometry geom.T
	if err := geojson.Unmarshal(b, &geometry); err != nil {
		return err
	}

	if geometry == nil {
		*g = Geometry{}
		return nil
	}

	*g = Geometry{Geometry: geometry, Valid: true}
	return nil
}

type GeometryCodec struct{}

func (GeometryCodec) FormatSupported(
	format int16,
) bool {

	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (GeometryCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (GeometryCodec) PlanEncode(
	_ *pgtype.Map, _ uint32, format int16, value any,
) pgtype.EncodePlan {

	if _, ok := value.(GeometryValuer); !ok {
		return nil
	}

	switch format {
	case pgtype.BinaryFormatCode:
		return encodePlanGeometryCodecBinary{}
	case pgtype.TextFormatCode:
		return encodePlanGeometryCodecText{}
	}

	return nil
}

type encodePlanGeometryCodecBinary struct{}

func (encodePlanGeometryCodecBinary) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	geometry, err := value.(GeometryValuer).GeometryValue()
	if err != nil {
		return nil, err
	}

	if !geometry.Valid {
		return nil, nil
	}

	data, err := ewkb.Marshal(geometry.Geometry, binary.BigEndian)
	if err != nil {
		return nil, err
	}
	buf = append(buf, data...)
	return buf, nil
}

type encodePlanGeometryCodecText struct{}

func (encodePlanGeometryCodecText) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	geometry, err := value.(GeometryValuer).GeometryValue()
	if err != nil {
		return nil, err
	}

	if !geometry.Valid {
		return nil, nil
	}

	data, err := ewkb.Marshal(geometry.Geometry, binary.BigEndian)
	if err != nil {
		return nil, err
	}
	buf = append(buf, hex.EncodeToString(data)...)
	return buf, nil
}

func (GeometryCodec) PlanScan(
	_ *pgtype.Map, _ uint32, format int16, target any,
) pgtype.ScanPlan {

	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case GeometryScanner:
			return scanPlanBinaryGeometryToGeometryScanner{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case GeometryScanner:
			return scanPlanTextGeometryToGeometryScanner{}
		}
	}

	return nil
}

type scanPlanBinaryGeometryToGeometryScanner struct{}

func (scanPlanBinaryGeometryToGeometryScanner) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(GeometryScanner)

	if src == nil {
		return scanner.ScanGeometry(Geometry{})
	}

	if len(src) < 2 {
		return fmt.Errorf("invalid length for ltree: %v", len(src))
	}

	version := src[0]
	if version != 1 {
		return fmt.Errorf("unsupported version for ltree: %v", version)
	}

	geometry, err := ewkb.Unmarshal(src)
	if err != nil {
		return err
	}

	return scanner.ScanGeometry(Geometry{Geometry: geometry, Valid: true})
}

type scanPlanTextGeometryToGeometryScanner struct{}

func (scanPlanTextGeometryToGeometryScanner) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(GeometryScanner)

	if src == nil {
		return scanner.ScanGeometry(Geometry{})
	}

	b, err := hex.DecodeString(string(src))
	if err != nil {
		return err
	}

	geometry, err := ewkb.Unmarshal(b)
	if err != nil {
		return err
	}

	return scanner.ScanGeometry(Geometry{Geometry: geometry, Valid: true})
}

func (c GeometryCodec) DecodeDatabaseSQLValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (driver.Value, error) {

	if src == nil {
		return nil, nil
	}

	var geometry Geometry
	err := codecScan(c, m, oid, format, src, &geometry)
	if err != nil {
		return nil, err
	}
	return geometry, nil
}

func (c GeometryCodec) DecodeValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (any, error) {

	if src == nil {
		return nil, nil
	}

	var geometry Geometry
	err := codecScan(c, m, oid, format, src, &geometry)
	if err != nil {
		return nil, err
	}
	return geometry, nil
}
