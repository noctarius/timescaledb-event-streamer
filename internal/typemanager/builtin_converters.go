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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hashicorp/go-uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/samber/lo"
	"math"
	"math/big"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"time"
)

var (
	microsPerHour = time.Hour.Microseconds()
	microsPerDay  = microsPerHour * 24

	unixEpoch = time.Unix(0, 0)

	avgDaysPerMonth       = 365.25 / 12
	avgMicrosDaysPerMonth = avgDaysPerMonth * float64(microsPerDay)

	timestampAsTextFormat = "2006-01-02T15:04:05.999999"
)

type rangeValueTransformer = func(value any) (string, error)

func arrayConverter[T any](
	oidElement uint32, elementConverter pgtypes.TypeConverter,
) pgtypes.TypeConverter {

	targetType := reflect.TypeOf(*new(T))
	return reflectiveArrayConverter(oidElement, targetType, elementConverter)
}

func reflectiveArrayConverter(
	oidElement uint32, targetType reflect.Type, elementConverter pgtypes.TypeConverter,
) pgtypes.TypeConverter {

	if targetType.Kind() != reflect.Array && targetType.Kind() != reflect.Slice {
		panic(fmt.Sprintf("arrayConverter needs array / slice type but got %s", targetType.String()))
	}
	targetElementType := targetType.Elem()
	targetSliceType := reflect.SliceOf(targetElementType)
	return func(_ uint32, value any) (any, error) {
		sourceValue := reflect.ValueOf(value)
		sourceLength := sourceValue.Len()

		// Create target slice
		targetValue := reflect.MakeSlice(targetSliceType, sourceLength, sourceLength)
		for i := 0; i < sourceLength; i++ {
			// Retrieve index value from source
			sourceIndex := sourceValue.Index(i)

			// Unwrap the source entry
			var element = sourceIndex.Interface()
			if elementConverter != nil {
				v, err := elementConverter(oidElement, element)
				if err != nil {
					return nil, err
				}
				element = v
			}

			// Set in target slice
			targetValue.Index(i).Set(
				reflect.ValueOf(element).Convert(targetElementType),
			)
		}
		return targetValue.Interface(), nil
	}
}

func enum2string(
	_ uint32, value any,
) (any, error) {

	switch v := value.(type) {
	case string:
		return v, nil
	}
	return nil, errIllegalValue
}

func float42float(
	_ uint32, value any,
) (any, error) {

	switch v := value.(type) {
	case pgtype.Float4:
		return v.Float32, nil
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	}
	return nil, errIllegalValue
}

func float82float(
	_ uint32, value any,
) (any, error) {

	switch v := value.(type) {
	case pgtype.Float8:
		return v.Float64, nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	}
	return nil, errIllegalValue
}

func date2int32(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(pgtype.Date); ok {
		value = v.Time
	}
	if v, ok := value.(time.Time); ok {
		return int32(int64(v.Sub(unixEpoch).Hours()) / 24), nil
	}
	return nil, errIllegalValue
}

func char2text(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(int32); ok {
		return fmt.Sprintf("%c", v), nil
	}
	return nil, errIllegalValue
}

func timestamp2text(
	oid uint32, value any,
) (any, error) {

	if v, ok := value.(time.Time); ok {
		switch oid {
		case pgtype.DateOID:
			return v.Format(time.DateOnly), nil
		case pgtype.TimestampOID:
			return v.In(time.UTC).Format(timestampAsTextFormat), nil
		default:
			return v.In(time.UTC).Format(time.RFC3339Nano), nil
		}
	}
	return nil, errIllegalValue
}

func time2text(
	_ uint32, value any,
) (any, error) {

	var micros int64
	if v, ok := value.(pgtype.Time); ok {
		micros = v.Microseconds
	} else if v, ok := value.(pgtypes.Timetz); ok {
		micros = v.Time.UnixMicro()
	} else {
		return nil, errIllegalValue
	}

	remaining := int64(time.Microsecond) * micros
	hours := remaining / int64(time.Hour)
	remaining = remaining % int64(time.Hour)
	minutes := remaining / int64(time.Minute)
	remaining = remaining % int64(time.Minute)
	seconds := remaining / int64(time.Second)
	remaining = remaining % int64(time.Second)
	return fmt.Sprintf(
		"%02d:%02d:%02d.%06d", hours, minutes, seconds,
		(time.Nanosecond * time.Duration(remaining)).Microseconds(),
	), nil
}

func timestamp2int64(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(time.Time); ok {
		return v.UnixMilli(), nil
	}
	return nil, errIllegalValue
}

func bits2string(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(pgtype.Bits); ok {
		if !v.Valid {
			return nil, nil
		}

		builder := strings.Builder{}

		remaining := v.Len
		for _, b := range v.Bytes {
			length := lo.Min([]int32{remaining, 8})
			for i := int32(0); i < length; i++ {
				zeroOrOne := b >> (7 - i) & 1
				builder.WriteString(fmt.Sprintf("%c", '0'+zeroOrOne))
			}
			remaining -= length
		}
		return builder.String(), nil
	}
	return nil, errIllegalValue
}

func json2text(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(map[string]any); ok {
		d, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(d), nil
	}
	return nil, errIllegalValue
}

func uuid2text(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(pgtype.UUID); ok {
		u, err := uuid.FormatUUID(v.Bytes[:])
		if err != nil {
			return nil, err
		}
		return u, nil
	} else if v, ok := value.([16]byte); ok {
		u, err := uuid.FormatUUID(v[:])
		if err != nil {
			return nil, err
		}
		return u, nil
	}
	return nil, errIllegalValue
}

func uint322int64(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(uint32); ok {
		return int64(v), nil
	}
	return nil, errIllegalValue
}

func macaddr2text(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(net.HardwareAddr); ok {
		return strings.ToLower(v.String()), nil
	}
	return nil, errIllegalValue
}

func addr2text(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(netip.Prefix); ok {
		return v.String(), nil
	}
	return nil, errIllegalValue
}

func interval2int64(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(pgtype.Interval); ok {
		return v.Microseconds +
			(int64(v.Days) * microsPerDay) +
			int64(math.Round(float64(v.Months)*avgMicrosDaysPerMonth)), nil
	}
	return nil, errIllegalValue
}

func numeric2float64(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(pgtype.Numeric); ok {
		f, err := v.Float64Value()
		if err != nil {
			return nil, err
		}
		return f.Float64, nil
	}
	return nil, errIllegalValue
}

func bytes2hexstring(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.([]byte); ok {
		return hex.EncodeToString(v), nil
	}
	return nil, errIllegalValue
}

func ltree2string(
	_ uint32, value any,
) (any, error) {

	if v, ok := value.(pgtypes.Ltree); ok {
		if !v.Valid {
			return nil, nil
		}
		return v.Path, nil
	}
	return nil, errIllegalValue
}

func numrange2string(
	_ uint32, value any,
) (any, error) {

	return range2string(value, func(value any) (string, error) {
		switch v := value.(type) {
		case float64:
			return fmt.Sprintf("%f", v), nil
		case big.Int:
			return v.String(), nil
		case pgtype.Numeric:
			f, err := v.Float64Value()
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%g", f.Float64), nil
		default:
			return "", errors.Errorf("not a numeric value: %+v", v)
		}
	})
}

func intrange2string(
	_ uint32, value any,
) (any, error) {

	return range2string(value, func(value any) (string, error) {
		return fmt.Sprintf("%d", value), nil
	})
}

func timestamprange2string(
	oid uint32, value any,
) (any, error) {

	return range2string(value, func(value any) (string, error) {
		if oid == pgtype.TstzrangeOID {
			oid = pgtype.TimestamptzOID
		} else if oid == pgtype.TsrangeOID {
			oid = pgtype.TimestampOID
		} else if oid == pgtype.DaterangeOID {
			oid = pgtype.DateOID
		}
		s, err := timestamp2text(oid, value)
		if err != nil {
			return "", err
		}
		return s.(string), nil
	})
}

func range2string(
	value any, transformer rangeValueTransformer,
) (any, error) {

	if v, ok := value.(pgtype.Range[any]); ok {
		lowerBound := "("
		if v.LowerType == pgtype.Inclusive {
			lowerBound = "["
		}
		upperBound := ")"
		if v.UpperType == pgtype.Inclusive {
			upperBound = "]"
		}

		lower := ""
		if v.LowerType != pgtype.Unbounded {
			l, err := transformer(v.Lower)
			if err != nil {
				return nil, err
			}
			lower = l
		}

		upper := ""
		if v.UpperType != pgtype.Unbounded {
			u, err := transformer(v.Upper)
			if err != nil {
				return nil, err
			}
			upper = u
		}

		return fmt.Sprintf("%s%s,%s%s", lowerBound, lower, upper, upperBound), nil
	}
	return nil, errIllegalValue
}
