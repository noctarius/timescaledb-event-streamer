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

/*type lazyCustomTypeConverter struct {
	typeManager *typeManager
	oidElement  uint32
	converter   pgtypes.TypeConverter
}

func (lctc *lazyCustomTypeConverter) convert(
	oid uint32, value any,
) (any, error) {
	if lctc.converter == nil {
		typ, err := lctc.typeManager.ResolveDataType(lctc.oidElement)
		if err != nil {
			return nil, err
		}

		if typ.Kind() == pgtypes.EnumKind {
			lctc.converter = enum2string
		} else {
			return nil, errIllegalValue
		}
	}

	return lctc.converter(oid, value)
}*/

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
	case schema.MAP:
		return mapType, nil
	default:
		return nil, errors.Errorf("Unsupported schema type %s", string(schemaType))
	}
}
