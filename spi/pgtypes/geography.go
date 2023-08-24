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

type GeographyScanner interface {
	ScanGeography(
		v Geography,
	) error
}

type GeographyValuer interface {
	GeographyValue() (Geography, error)
}

type Geography struct {
	Geography geom.T
	Valid     bool
}

func (g *Geography) ScanGeography(
	v Geography,
) error {

	*g = v
	return nil
}

func (g Geography) GeographyValue() (Geography, error) {
	return g, nil
}

func (g *Geography) Scan(
	src any,
) error {

	if src == nil {
		*g = Geography{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextGeographyToGeographyScanner{}.Scan([]byte(src), g)
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (g Geography) Value() (driver.Value, error) {
	if !g.Valid {
		return nil, nil
	}

	return g.Geography, nil
}

func (g Geography) MarshalJSON() ([]byte, error) {
	if !g.Valid {
		return []byte("null"), nil
	}

	return geojson.Marshal(g.Geography)
}

func (g *Geography) UnmarshalJSON(
	b []byte,
) error {

	var geography geom.T
	if err := geojson.Unmarshal(b, &geography); err != nil {
		return err
	}

	if geography == nil {
		*g = Geography{}
		return nil
	}

	*g = Geography{Geography: geography, Valid: true}
	return nil
}

type GeographyCodec struct{}

func (GeographyCodec) FormatSupported(
	format int16,
) bool {

	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (GeographyCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (GeographyCodec) PlanEncode(
	_ *pgtype.Map, _ uint32, format int16, value any,
) pgtype.EncodePlan {

	if _, ok := value.(GeographyValuer); !ok {
		return nil
	}

	switch format {
	case pgtype.BinaryFormatCode:
		return encodePlanGeographyCodecBinary{}
	case pgtype.TextFormatCode:
		return encodePlanGeographyCodecText{}
	}

	return nil
}

type encodePlanGeographyCodecBinary struct{}

func (encodePlanGeographyCodecBinary) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	geography, err := value.(GeographyValuer).GeographyValue()
	if err != nil {
		return nil, err
	}

	if !geography.Valid {
		return nil, nil
	}

	data, err := ewkb.Marshal(geography.Geography, binary.BigEndian)
	if err != nil {
		return nil, err
	}
	buf = append(buf, data...)
	return buf, nil
}

type encodePlanGeographyCodecText struct{}

func (encodePlanGeographyCodecText) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	geography, err := value.(GeographyValuer).GeographyValue()
	if err != nil {
		return nil, err
	}

	if !geography.Valid {
		return nil, nil
	}

	data, err := ewkb.Marshal(geography.Geography, binary.BigEndian)
	if err != nil {
		return nil, err
	}
	buf = append(buf, hex.EncodeToString(data)...)
	return buf, nil
}

func (GeographyCodec) PlanScan(
	_ *pgtype.Map, _ uint32, format int16, target any,
) pgtype.ScanPlan {

	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case GeographyScanner:
			return scanPlanBinaryGeographyToGeographyScanner{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case GeographyScanner:
			return scanPlanTextGeographyToGeographyScanner{}
		}
	}

	return nil
}

type scanPlanBinaryGeographyToGeographyScanner struct{}

func (scanPlanBinaryGeographyToGeographyScanner) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(GeographyScanner)

	if src == nil {
		return scanner.ScanGeography(Geography{})
	}

	if len(src) < 2 {
		return fmt.Errorf("invalid length for ltree: %v", len(src))
	}

	version := src[0]
	if version != 1 {
		return fmt.Errorf("unsupported version for ltree: %v", version)
	}

	geography, err := ewkb.Unmarshal(src)
	if err != nil {
		return err
	}

	return scanner.ScanGeography(Geography{Geography: geography, Valid: true})
}

type scanPlanTextGeographyToGeographyScanner struct{}

func (scanPlanTextGeographyToGeographyScanner) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(GeographyScanner)

	if src == nil {
		return scanner.ScanGeography(Geography{})
	}

	b, err := hex.DecodeString(string(src))
	if err != nil {
		return err
	}

	geography, err := ewkb.Unmarshal(b)
	if err != nil {
		return err
	}

	return scanner.ScanGeography(Geography{Geography: geography, Valid: true})
}

func (c GeographyCodec) DecodeDatabaseSQLValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (driver.Value, error) {

	if src == nil {
		return nil, nil
	}

	var geography Geography
	err := codecScan(c, m, oid, format, src, &geography)
	if err != nil {
		return nil, err
	}
	return geography, nil
}

func (c GeographyCodec) DecodeValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (any, error) {

	if src == nil {
		return nil, nil
	}

	var geography Geography
	err := codecScan(c, m, oid, format, src, &geography)
	if err != nil {
		return nil, err
	}
	return geography, nil
}
