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
)

type Geography struct {
	Geography geom.T
	Valid     bool
}

func (g Geography) value() geom.T {
	return g.Geography
}

func (g Geography) valid() bool {
	return g.Valid
}

func (g Geography) New(
	v geom.T,
) Geography {

	return Geography{Geography: v, Valid: true}
}

func (g *Geography) ScanPostGisValue(
	v Geography,
) error {

	*g = v
	return nil
}

func (g Geography) PostGisValue() (Geography, error) {
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
		return scanPlanTextPostGisToPostGisScanner[Geography, *Geography]{}.Scan([]byte(src), g)
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
	return postGisMarshalJson(g)
}

func (g *Geography) UnmarshalJSON(
	b []byte,
) error {

	geography, err := postGisUnmarshalJson(b)
	if err != nil {
		return err
	}

	if geography == nil {
		*g = Geography{}
		return nil
	}

	*g = Geography{Geography: geography, Valid: true}
	return nil
}
