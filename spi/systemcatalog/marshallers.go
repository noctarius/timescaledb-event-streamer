package systemcatalog

import (
	"encoding/json"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"time"
)

type BinaryMarshaller func(buffer encoding.WriteBuffer, oid uint32, value any) error

type BinaryUnmarshaller func(buffer encoding.ReadBuffer, oid uint32) (value any, err error)

var marshallers = map[uint32]BinaryMarshaller{
	pgtype.BoolOID:        boolMarshaller,
	pgtype.BitOID:         boolMarshaller,
	pgtype.BitArrayOID:    bitArrayMarshaller,
	pgtype.Int2OID:        intMarshaller,
	pgtype.Int4OID:        intMarshaller,
	pgtype.Int8OID:        intMarshaller,
	pgtype.Float4OID:      floatMarshaller,
	pgtype.Float8OID:      floatMarshaller,
	pgtype.QCharOID:       stringMarshaller,
	pgtype.VarcharOID:     stringMarshaller,
	pgtype.TextOID:        stringMarshaller,
	pgtype.TimestampOID:   timestampMarshaller,
	pgtype.TimestamptzOID: timestampMarshaller,
	pgtype.IntervalOID:    intervalMarshaller,
	pgtype.ByteaArrayOID:  byteArrayMarshaller,
	pgtype.JSONOID:        jsonMarshaller,
	pgtype.JSONBOID:       jsonMarshaller,
	pgtype.UUIDOID:        uuidMarshaller,
	pgtype.PointOID:       nil, // FIXME
	pgtype.NumericOID:     nil, // FIXME
	514836:                nil, // geometry, FIXME
	516272:                nil, // ltree, FIXME
}

var unmarshallers = map[uint32]BinaryUnmarshaller{
	pgtype.BoolOID:        boolUnmarshaller,
	pgtype.BitOID:         boolUnmarshaller,
	pgtype.BitArrayOID:    bitArrayUnmarshaller,
	pgtype.Int2OID:        intUnmarshaller,
	pgtype.Int4OID:        intUnmarshaller,
	pgtype.Int8OID:        intUnmarshaller,
	pgtype.Float4OID:      floatUnmarshaller,
	pgtype.Float8OID:      floatUnmarshaller,
	pgtype.QCharOID:       stringUnmarshaller,
	pgtype.VarcharOID:     stringUnmarshaller,
	pgtype.TextOID:        stringUnmarshaller,
	pgtype.TimestampOID:   timestampUnmarshaller,
	pgtype.TimestamptzOID: timestampUnmarshaller,
	pgtype.IntervalOID:    intervalUnmarshaller,
	pgtype.ByteaArrayOID:  byteArrayUnmarshaller,
	pgtype.JSONOID:        jsonUnmarshaller,
	pgtype.JSONBOID:       jsonUnmarshaller,
	pgtype.UUIDOID:        uuidUnmarshaller,
	pgtype.PointOID:       nil, // FIXME
	pgtype.NumericOID:     nil, // FIXME
	514836:                nil, // geometry, FIXME
	516272:                nil, // ltree, FIXME
}

func BinaryMarshall(buffer encoding.WriteBuffer, oid uint32, value any) error {
	if marshaller, found := marshallers[oid]; found {
		return marshaller(buffer, oid, value)
	}
	return ErrIllegalValue
}

func BinaryUnmarshall(buffer encoding.ReadBuffer, oid uint32) (value any, err error) {
	if unmarshaller, found := unmarshallers[oid]; found {
		return unmarshaller(buffer, oid)
	}
	return nil, ErrIllegalValue
}

func boolMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	switch v := value.(type) {
	case bool:
		return buffer.PutBool(v)
	case *bool:
		return buffer.PutBool(*v)
	default:
		return ErrIllegalValue
	}

}

func boolUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	return buffer.ReadBool()
}

func bitArrayMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if data, ok := value.([]byte); ok {
		return buffer.PutBytes(data)
	}
	return ErrIllegalValue
}

func bitArrayUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	return buffer.ReadBytes()
}

func intMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	switch v := value.(type) {
	case int8:
		return buffer.PutInt8(v)
	case *int8:
		return buffer.PutInt8(*v)
	case int16:
		return buffer.PutInt16(v)
	case *int16:
		return buffer.PutInt16(*v)
	case int32:
		return buffer.PutInt32(v)
	case *int32:
		return buffer.PutInt32(*v)
	case int64:
		return buffer.PutInt64(v)
	case *int64:
		return buffer.PutInt64(*v)
	default:
		return ErrIllegalValue
	}
}

func intUnmarshaller(buffer encoding.ReadBuffer, oid uint32) (value any, err error) {
	switch oid {
	case pgtype.Int2OID:
		return buffer.ReadInt16()
	case pgtype.Int4OID:
		return buffer.ReadInt32()
	case pgtype.Int8OID:
		return buffer.ReadInt64()
	default:
		return 0, ErrIllegalValue
	}
}

func floatMarshaller(buffer encoding.WriteBuffer, oid uint32, value any) error {
	switch v := value.(type) {
	case float32:
		return buffer.PutFloat32(v)
	case *float32:
		return buffer.PutFloat32(*v)
	case float64:
		return buffer.PutFloat64(v)
	case *float64:
		return buffer.PutFloat64(*v)
	default:
		return ErrIllegalValue
	}
}

func floatUnmarshaller(buffer encoding.ReadBuffer, oid uint32) (value any, err error) {
	switch oid {
	case pgtype.Float4OID:
		return buffer.ReadFloat32()
	case pgtype.Float8OID:
		return buffer.ReadFloat64()
	default:
		return 0, ErrIllegalValue
	}
}

func stringMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if s, ok := value.(string); ok {
		return buffer.PutString(s)
	}
	return ErrIllegalValue
}

func stringUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	return buffer.ReadString()
}

func timestampMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if t, ok := value.(time.Time); ok {
		if data, err := t.MarshalBinary(); err == nil {
			return buffer.PutBytes(data)
		} else {
			return errors.Wrap(err, 0)
		}
	}
	return ErrIllegalValue
}

func timestampUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	data, err := buffer.ReadBytes()
	if err != nil {
		return nil, err
	}
	t := time.Time{}
	if err := t.UnmarshalBinary(data); err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return t, nil
}

func intervalMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if d, ok := value.(time.Duration); ok {
		return buffer.PutInt64(int64(d))
	}
	return ErrIllegalValue
}

func intervalUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	d, err := buffer.ReadInt64()
	if err != nil {
		return time.Second * 0, err
	}
	return time.Duration(d), nil
}

func byteArrayMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if data, ok := value.([]byte); ok {
		return buffer.PutBytes(data)
	}
	return ErrIllegalValue
}

func byteArrayUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	return buffer.ReadBytes()
}

func jsonMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if v, ok := value.(map[string]any); ok {
		d, err := json.Marshal(v)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		return buffer.PutBytes(d)
	}
	return ErrIllegalValue
}

func jsonUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	d, err := buffer.ReadBytes()
	if err != nil {
		return nil, err
	}
	data := make(map[string]any)
	if err := json.Unmarshal(d, &data); err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return data, nil
}

func uuidMarshaller(buffer encoding.WriteBuffer, _ uint32, value any) error {
	if u, ok := value.(pgtype.UUID); ok {
		if err := buffer.PutBytes(u.Bytes[:]); err != nil {
			return err
		}
		return nil
	}
	return ErrIllegalValue
}

func uuidUnmarshaller(buffer encoding.ReadBuffer, _ uint32) (value any, err error) {
	d, err := buffer.ReadBytes()
	if err != nil {
		return nil, err
	}
	var v pgtype.UUID
	copy(v.Bytes[:], d)
	v.Valid = true
	return v, nil
}
