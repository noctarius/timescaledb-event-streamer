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
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"reflect"
)

type lazyArrayConverter struct {
	typeManager *typeManager
	oidElement  uint32
	converter   pgtypes.TypeConverter
}

func (lac *lazyArrayConverter) convert(
	oid uint32, value any,
) (any, error) {

	if lac.converter == nil {
		elementType, err := lac.typeManager.ResolveDataType(lac.oidElement)
		if err != nil {
			return nil, err
		}

		elementConverter, err := lac.typeManager.ResolveTypeConverter(lac.oidElement)
		if err != nil {
			return nil, err
		}

		reflectiveType, err := schemaType2ReflectiveType(elementType.SchemaType())
		if err != nil {
			return nil, err
		}

		targetType := reflect.SliceOf(reflectiveType)
		lac.converter = reflectiveArrayConverter(lac.oidElement, targetType, elementConverter)
	}

	return lac.converter(oid, value)
}

func schemaType2ReflectiveType(
	schemaType schema.Type,
) (reflect.Type, error) {

	switch schemaType {
	case schema.INT8:
		return int8Type, nil
	case schema.INT16:
		return int16Type, nil
	case schema.INT32:
		return int32Type, nil
	case schema.INT64:
		return int64Type, nil
	case schema.FLOAT32:
		return float32Type, nil
	case schema.FLOAT64:
		return float64Type, nil
	case schema.BOOLEAN:
		return booleanType, nil
	case schema.STRING:
		return stringType, nil
	case schema.BYTES:
		return byteaType, nil
	case schema.MAP, schema.STRUCT:
		return mapType, nil
	default:
		return nil, errors.Errorf("Unsupported schema type %s", string(schemaType))
	}
}
