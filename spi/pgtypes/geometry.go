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
	"fmt"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

type Geometry struct {
	Geometry geom.T
	Valid    bool
}

func (g Geometry) value() geom.T {
	return g.Geometry
}

func (g Geometry) valid() bool {
	return g.Valid
}

func (g Geometry) New(
	v geom.T,
) Geometry {

	return Geometry{Geometry: v, Valid: true}
}

func (g *Geometry) ScanPostGisValue(
	v Geometry,
) error {

	*g = v
	return nil
}

func (g Geometry) PostGisValue() (Geometry, error) {
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
		return scanPlanTextPostGisToPostGisScanner[Geography, *Geography]{}.Scan([]byte(src), g)
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (g Geometry) Value() (driver.Value, error) {
	return postGisMarshalJson(g)
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

	geometry, err := postGisUnmarshalJson(b)
	if err != nil {
		return err
	}

	if geometry == nil {
		*g = Geometry{}
		return nil
	}

	*g = Geometry{Geometry: geometry, Valid: true}
	return nil
}
