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

func EnhancedArrayTextCodecFactory[V any](
	typeMap *pgtype.Map, typ PgType,
) pgtype.Codec {

	if pt, present := typeMap.TypeForOID(typ.OidElement()); present {
		scanPlan := typeMap.PlanScan(typ.OidElement(), pgtype.TextFormatCode, new(V))
		return &EnhancedArrayTextCodec[V]{
			PgxArrayCodec:  &pgtype.ArrayCodec{ElementType: pt},
			oid:            typ.Oid(),
			oidElem:        typ.OidElement(),
			delimiter:      typ.Delimiter(),
			cachedScanPlan: scanPlan,
			planValueType:  *new(V),
		}
	}
	return nil
}

// EnhancedArrayTextCodec is an enhanced array codec utilizing the
// typdelim field from pg_type as a separator of the text value
// representing the array in the PG wire protocol. The pgx internal
// implementation always expects the delimiter to be a comma (,)
// which isn't true for all types, such as box, and PostGIS types.
//
// This special version only implements text format, binary is
// forwarded to the original array codec, as it doesn't have the
// same issue, since it's a text parsing issue only.
type EnhancedArrayTextCodec[V any] struct {
	PgxArrayCodec  *pgtype.ArrayCodec
	oid            uint32
	oidElem        uint32
	delimiter      string
	cachedScanPlan pgtype.ScanPlan
	planValueType  V
}

func (c *EnhancedArrayTextCodec[V]) FormatSupported(
	format int16,
) bool {

	if format == pgtype.TextFormatCode {
		return true
	}
	return c.PgxArrayCodec.FormatSupported(format)
}

func (c *EnhancedArrayTextCodec[V]) PreferredFormat() int16 {
	return c.PgxArrayCodec.PreferredFormat()
}

func (c *EnhancedArrayTextCodec[V]) PlanEncode(
	m *pgtype.Map, oid uint32, format int16, value any,
) pgtype.EncodePlan {

	return c.PgxArrayCodec.PlanEncode(m, oid, format, value)
}

func (c *EnhancedArrayTextCodec[V]) PlanScan(
	m *pgtype.Map, oid uint32, format int16, target any,
) pgtype.ScanPlan {

	if oid == c.oid && format == pgtype.TextFormatCode {
		return &scanPlanEnhancedArrayTextCodec[V]{
			typeMap:        m,
			oidElem:        c.oidElem,
			delimiter:      c.delimiter,
			planValueType:  c.planValueType,
			cachedScanPlan: c.cachedScanPlan,
		}
	}

	return c.PgxArrayCodec.PlanScan(m, oid, format, target)
}

func (c *EnhancedArrayTextCodec[V]) DecodeDatabaseSQLValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (driver.Value, error) {

	return c.PgxArrayCodec.DecodeDatabaseSQLValue(m, oid, format, src)
}

func (c *EnhancedArrayTextCodec[V]) DecodeValue(
	m *pgtype.Map, oid uint32, format int16, src []byte,
) (any, error) {

	if src == nil {
		return nil, nil
	}

	if format == pgtype.TextFormatCode {
		var slice []V
		err := m.PlanScan(oid, format, &slice).Scan(src, &slice)
		return slice, err
	}

	return c.PgxArrayCodec.DecodeValue(m, oid, format, src)
}

type scanPlanEnhancedArrayTextCodec[V any] struct {
	cachedScanPlan pgtype.ScanPlan
	typeMap        *pgtype.Map
	oidElem        uint32
	delimiter      string
	planValueType  V
}

func (spbac *scanPlanEnhancedArrayTextCodec[V]) Scan(
	src []byte, dst any,
) error {

	array := dst.(*[]V)

	scanPlan := spbac.cachedScanPlan
	if scanPlan == nil {
		scanPlan = spbac.typeMap.PlanScan(spbac.oidElem, pgtype.TextFormatCode, &spbac.planValueType)
	}

	// Semicolon seems to be the separator here
	elements := strings.Split(string(src[1:len(src)-1]), spbac.delimiter)
	for _, element := range elements {
		item := new(V)
		if err := scanPlan.Scan([]byte(element), item); err != nil {
			return err
		}
		*array = append(*array, *item)
	}
	return nil
}
