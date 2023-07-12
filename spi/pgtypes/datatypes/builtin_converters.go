package datatypes

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"net"
	"net/netip"
	"reflect"
	"time"
)

func arrayConverter[T any](oidElement uint32, elementConverter Converter) Converter {
	targetType := reflect.TypeOf(*new(T))
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
			v, err := elementConverter(oidElement, sourceIndex.Interface())
			if err != nil {
				return nil, err
			}

			// Set in target slice
			targetValue.Index(i).Set(
				reflect.ValueOf(v).Convert(targetElementType),
			)
		}
		return targetValue.Interface(), nil
	}
}

func char2text(_ uint32, value any) (any, error) {
	if v, ok := value.(int32); ok {
		return string(v), nil
	}
	return nil, ErrIllegalValue
}

func timestamp2text(oid uint32, value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		switch oid {
		case pgtype.DateOID:
			return v.Format(time.DateOnly), nil
		default:
			return v.In(time.UTC).Format(time.RFC3339Nano), nil
		}
	}
	return nil, ErrIllegalValue
}

func time2text(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Time); ok {
		remaining := int64(time.Microsecond) * v.Microseconds
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
	return nil, ErrIllegalValue
}

func timestamp2int64(_ uint32, value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		return v.UnixMilli(), nil
	}
	return nil, ErrIllegalValue
}

/*func bit2bool(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Bits); ok {
		return v.Bytes[0]&0xF0 == 128, nil
	}
	return nil, ErrIllegalValue
}

func bits2bytes(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Bits); ok {
		return v.Bytes, nil
	}
	return nil, ErrIllegalValue
}*/

func json2text(_ uint32, value any) (any, error) {
	if v, ok := value.(map[string]any); ok {
		d, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(d), nil
	}
	return nil, ErrIllegalValue
}

func uuid2text(_ uint32, value any) (any, error) {
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
	return nil, ErrIllegalValue
}

func uint322int64(_ uint32, value any) (any, error) {
	if v, ok := value.(uint32); ok {
		return int64(v), nil
	}
	return nil, ErrIllegalValue
}

func macaddr2text(_ uint32, value any) (any, error) {
	if v, ok := value.(net.HardwareAddr); ok {
		return v.String(), nil
	}
	return nil, ErrIllegalValue
}

func addr2text(_ uint32, value any) (any, error) {
	if v, ok := value.(netip.Prefix); ok {
		return v.String(), nil
	}
	return nil, ErrIllegalValue
}

func interval2int64(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Interval); ok {
		return v.Microseconds, nil
	}
	return nil, ErrIllegalValue
}

func numeric2variableScaleDecimal(_ uint32, value any) (any, error) {
	if v, ok := value.(pgtype.Numeric); ok {
		return schemamodel.Struct{
			"value": hex.EncodeToString(v.Int.Bytes()),
			"scale": v.Exp,
		}, nil
	}
	return nil, ErrIllegalValue
}
