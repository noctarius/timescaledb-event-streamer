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
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
)

type XmlScanner interface {
	ScanXml(
		v Xml,
	) error
}

type XmlValuer interface {
	XmlValue() (Xml, error)
}

type Xml struct {
	Xml   string
	Valid bool
}

func (x *Xml) ScanXml(
	v Xml,
) error {

	*x = v
	return nil
}

func (x Xml) XmlValue() (Xml, error) {
	return x, nil
}

func (x *Xml) Scan(
	src any,
) error {

	if src == nil {
		*x = Xml{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextXmlToXmlScanner{}.Scan([]byte(src), x)
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (x Xml) Value() (driver.Value, error) {
	if !x.Valid {
		return nil, nil
	}

	return x.Xml, nil
}

func (x Xml) MarshalJSON() ([]byte, error) {
	if !x.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(x.Xml)
}

func (x *Xml) UnmarshalJSON(
	b []byte,
) error {

	var s *string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	if s == nil {
		*x = Xml{}
		return nil
	}

	*x = Xml{Xml: *s, Valid: true}
	return nil
}

type XmlCodec struct{}

func (XmlCodec) FormatSupported(
	format int16,
) bool {

	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (XmlCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (XmlCodec) PlanEncode(
	_ *pgtype.Map, _ uint32, format int16, value any,
) pgtype.EncodePlan {

	if _, ok := value.(XmlValuer); !ok {
		return nil
	}

	switch format {
	case pgtype.BinaryFormatCode:
		return encodePlanXmlCodecBinary{}
	case pgtype.TextFormatCode:
		return encodePlanXmlCodecText{}
	}

	return nil
}

type encodePlanXmlCodecBinary struct{}

func (encodePlanXmlCodecBinary) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	xml, err := value.(XmlValuer).XmlValue()
	if err != nil {
		return nil, err
	}

	if !xml.Valid {
		return nil, nil
	}

	buf = append(buf, xml.Xml...)

	return buf, nil
}

type encodePlanXmlCodecText struct{}

func (encodePlanXmlCodecText) Encode(
	value any, buf []byte,
) (newBuf []byte, err error) {

	xml, err := value.(XmlValuer).XmlValue()
	if err != nil {
		return nil, err
	}

	if !xml.Valid {
		return nil, nil
	}

	buf = append(buf, xml.Xml...)

	return buf, nil
}

func (XmlCodec) PlanScan(
	_ *pgtype.Map, _ uint32, format int16, target any,
) pgtype.ScanPlan {

	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case XmlScanner:
			return scanPlanBinaryXmlToXmlScanner{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case XmlScanner:
			return scanPlanTextXmlToXmlScanner{}
		}
	}

	return nil
}

type scanPlanBinaryXmlToXmlScanner struct{}

func (scanPlanBinaryXmlToXmlScanner) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(XmlScanner)

	if src == nil {
		return scanner.ScanXml(Xml{})
	}

	if len(src) < 2 {
		return fmt.Errorf("invalid length for xml: %v", len(src))
	}

	return scanner.ScanXml(Xml{Xml: string(src), Valid: true})
}

type scanPlanTextXmlToXmlScanner struct{}

func (scanPlanTextXmlToXmlScanner) Scan(
	src []byte, dst any,
) error {

	scanner := (dst).(XmlScanner)

	if src == nil {
		return scanner.ScanXml(Xml{})
	}

	return scanner.ScanXml(Xml{Xml: string(src), Valid: true})
}

func (c XmlCodec) DecodeDatabaseSQLValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (driver.Value, error) {

	if src == nil {
		return nil, nil
	}

	var xml Xml
	err := codecScan(c, m, oid, format, src, &xml)
	if err != nil {
		return nil, err
	}
	return xml, nil
}

func (c XmlCodec) DecodeValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (any, error) {

	if src == nil {
		return nil, nil
	}

	var xml Xml
	err := codecScan(c, m, oid, format, src, &xml)
	if err != nil {
		return nil, err
	}
	return xml, nil
}
