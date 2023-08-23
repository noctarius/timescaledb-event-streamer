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
	"github.com/jackc/pgx/v5/pgtype"
	"strings"
)

// GeometryArrayCodec is a wrapper codec for geometry[] which isn't handled gracefully in
// the pgx base source code (at least in PG < 14 without binary wire protocol
// support) due to the way the text protocol sends back the value of a geometry item.
type GeometryArrayCodec struct {
	PgxArrayCodec *pgtype.ArrayCodec
}

func (c *GeometryArrayCodec) FormatSupported(
	format int16,
) bool {

	if format == pgtype.TextFormatCode {
		return true
	}
	return c.PgxArrayCodec.FormatSupported(format)
}

func (c *GeometryArrayCodec) PreferredFormat() int16 {
	return c.PgxArrayCodec.PreferredFormat()
}

func (c *GeometryArrayCodec) PlanEncode(
	m *pgtype.Map, oid uint32, format int16, value any,
) pgtype.EncodePlan {

	return c.PgxArrayCodec.PlanEncode(m, oid, format, value)
}

func (c *GeometryArrayCodec) PlanScan(
	m *pgtype.Map, oid uint32, format int16, target any,
) pgtype.ScanPlan {

	if format == pgtype.TextFormatCode {
		return &scanPlanGeometryArrayTextCodec{
			typeMap: m,
			oid:     oid,
		}
	}

	return c.PgxArrayCodec.PlanScan(m, oid, format, target)
}

func (c *GeometryArrayCodec) DecodeDatabaseSQLValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (driver.Value, error) {

	return c.PgxArrayCodec.DecodeDatabaseSQLValue(m, oid, format, src)
}

func (c *GeometryArrayCodec) DecodeValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (any, error) {

	if src == nil {
		return nil, nil
	}

	if format == pgtype.TextFormatCode {
		var slice []Geometry
		err := m.PlanScan(oid, format, &slice).Scan(src, &slice)
		return slice, err
	}

	return c.PgxArrayCodec.DecodeValue(m, oid, format, src)
}

type scanPlanGeometryArrayTextCodec struct {
	geometryCodec GeometryCodec
	typeMap       *pgtype.Map
	oid           uint32
}

func (spbac *scanPlanGeometryArrayTextCodec) Scan(
	src []byte, dst any,
) error {

	array := dst.(*[]Geometry)

	scanPlan := spbac.geometryCodec.PlanScan(spbac.typeMap, spbac.oid, pgtype.TextFormatCode, &Geometry{})
	if scanPlan == nil {
		scanPlan = spbac.typeMap.PlanScan(spbac.oid, pgtype.TextFormatCode, &Geometry{})
	}

	// Semicolon seems to be the separator here
	elements := strings.Split(string(src[1:len(src)-1]), ":")
	for _, element := range elements {
		item := Geometry{}
		if err := scanPlan.Scan([]byte(element), &item); err != nil {
			return err
		}
		*array = append(*array, item)
	}
	return nil
}
