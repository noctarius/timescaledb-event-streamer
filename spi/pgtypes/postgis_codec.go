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

type PostGisScanner[T any] interface {
	ScanPostGisValue(T) error
	New(value geom.T) T
}

type PostGisValuer[T valuer] interface {
	PostGisValue() (T, error)
}

type valuer interface {
	valid() bool
	value() geom.T
}

func postGisMarshalJson(
	v valuer,
) ([]byte, error) {

	if !v.valid() {
		return []byte("null"), nil
	}

	return geojson.Marshal(v.value())
}

func postGisUnmarshalJson(
	b []byte,
) (geom.T, error) {

	var value geom.T
	if err := geojson.Unmarshal(b, &value); err != nil {
		return nil, err
	}

	if value == nil {
		return nil, nil
	}

	return value, nil
}

type PostGisCodec[V valuer, S PostGisScanner[V], E PostGisValuer[V]] struct{}

func (PostGisCodec[V, S, E]) FormatSupported(
	format int16,
) bool {

	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (PostGisCodec[V, S, E]) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (PostGisCodec[V, S, E]) PlanEncode(
	_ *pgtype.Map, _ uint32, format int16, value any,
) pgtype.EncodePlan {

	if _, ok := value.(E); !ok {
		return nil
	}

	switch format {
	case pgtype.BinaryFormatCode:
		return encodePlanPostGisCodecBinary[V, E]{}
	case pgtype.TextFormatCode:
		return encodePlanPostGisCodecText[V, E]{}
	}

	return nil
}

type encodePlanPostGisCodecBinary[V valuer, E PostGisValuer[V]] struct{}

func (encodePlanPostGisCodecBinary[V, W]) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	v, err := value.(W).PostGisValue()
	if err != nil {
		return nil, err
	}

	if !v.valid() {
		return nil, nil
	}

	data, err := ewkb.Marshal(v.value(), binary.BigEndian)
	if err != nil {
		return nil, err
	}
	buf = append(buf, data...)
	return buf, nil
}

type encodePlanPostGisCodecText[V valuer, E PostGisValuer[V]] struct{}

func (encodePlanPostGisCodecText[V, E]) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	v, err := value.(E).PostGisValue()
	if err != nil {
		return nil, err
	}

	if !v.valid() {
		return nil, nil
	}

	data, err := ewkb.Marshal(v.value(), binary.BigEndian)
	if err != nil {
		return nil, err
	}
	buf = append(buf, hex.EncodeToString(data)...)
	return buf, nil
}

func (PostGisCodec[V, S, E]) PlanScan(
	_ *pgtype.Map, _ uint32, format int16, target any,
) pgtype.ScanPlan {

	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case S:
			return scanPlanBinaryPostGisToPostGisScanner[V, S]{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case S:
			return scanPlanTextPostGisToPostGisScanner[V, S]{}
		}
	}

	return nil
}

type scanPlanBinaryPostGisToPostGisScanner[V any, S PostGisScanner[V]] struct{}

func (scanPlanBinaryPostGisToPostGisScanner[V, S]) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(S)

	if src == nil {
		return scanner.ScanPostGisValue(*new(V))
	}

	if len(src) < 2 {
		return fmt.Errorf("invalid length for PostGIS: %v", len(src))
	}

	version := src[0]
	if version != 1 {
		return fmt.Errorf("unsupported version for PostGIS: %v", version)
	}

	v, err := ewkb.Unmarshal(src)
	if err != nil {
		return err
	}

	return scanner.ScanPostGisValue(scanner.New(v))
}

type scanPlanTextPostGisToPostGisScanner[V any, S PostGisScanner[V]] struct{}

func (scanPlanTextPostGisToPostGisScanner[V, S]) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(S)

	if src == nil {
		return scanner.ScanPostGisValue(*new(V))
	}

	b, err := hex.DecodeString(string(src))
	if err != nil {
		return err
	}

	v, err := ewkb.Unmarshal(b)
	if err != nil {
		return err
	}

	return scanner.ScanPostGisValue(scanner.New(v))
}

func (c PostGisCodec[V, S, E]) DecodeDatabaseSQLValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (driver.Value, error) {

	if src == nil {
		return nil, nil
	}

	var v V
	err := codecScan(c, m, oid, format, src, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (c PostGisCodec[V, S, E]) DecodeValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (any, error) {

	if src == nil {
		return nil, nil
	}

	var v V
	err := codecScan(c, m, oid, format, src, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}
