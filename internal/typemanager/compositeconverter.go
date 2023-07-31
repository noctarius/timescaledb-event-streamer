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
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
)

func newCompositeConverter(
	typeManager *typeManager, typ pgtypes.PgType,
) (pgtypes.TypeConverter, error) {

	columns, err := typ.CompositeColumns()
	if err != nil {
		return nil, err
	}

	columnConverters := make(map[string]pgtypes.TypeConverter)
	for _, column := range columns {
		dataType := column.Type().Oid()
		converter, err := typeManager.ResolveTypeConverter(dataType)
		if err != nil {
			return nil, err
		}
		columnConverters[column.Name()] = func(_ uint32, value any) (any, error) {
			return converter(dataType, value)
		}
	}

	return func(_ uint32, value any) (any, error) {
		if m, ok := value.(map[string]any); ok {
			result := make(map[string]any)
			for k, v := range m {
				if converter, present := columnConverters[k]; present {
					// OID is already bound
					r, err := converter(0, v)
					if err != nil {
						return nil, err
					}
					v = r
				}
				result[k] = v
			}
			return result, nil
		}
		return nil, errIllegalValue
	}, nil
}
