package pgtypes

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgio"
	"github.com/jackc/pgx/v5/pgtype"
	"strings"
	"time"
)

const format = "15:04:05.999999999Z07:00"
const secondFormat = "15:04:05.999999999-07:00"

var (
	microsPerHour = time.Hour.Microseconds()
	microsPerDay  = microsPerHour * 24

	nanosPerMicros = time.Microsecond.Nanoseconds()
	nanosPerSecond = time.Second.Nanoseconds()
)

type TimetzScanner interface {
	ScanTimetz(v Timetz) error
}

type TimetzValuer interface {
	TimetzValue() (Timetz, error)
}

type Timetz struct {
	Time  time.Time
	Valid bool
}

func (ttz *Timetz) ScanTimetz(v Timetz) error {
	*ttz = v
	return nil
}

func (ttz Timetz) TimetzValue() (Timetz, error) {
	return ttz, nil
}

func (ttz *Timetz) Scan(src any) error {
	if src == nil {
		*ttz = Timetz{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextTimetzToTimetzScanner{}.Scan([]byte(src), ttz)
	case time.Time:
		*ttz = Timetz{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (ttz Timetz) Value() (driver.Value, error) {
	if !ttz.Valid {
		return nil, nil
	}

	return ttz.Time, nil
}

func (ttz Timetz) MarshalJSON() ([]byte, error) {
	if !ttz.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(ttz.Time.Format(format))
}

func (ttz *Timetz) UnmarshalJSON(b []byte) error {
	var s *string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	if s == nil {
		*ttz = Timetz{}
		return nil
	}

	var tim time.Time
	if strings.Contains(*s, "Z") {
		t, err := time.Parse(format, *s)
		if err != nil {
			return err
		}
		tim = t
	} else {
		t, err := time.Parse(secondFormat, *s)
		if err != nil {
			return err
		}
		tim = t
	}

	*ttz = Timetz{Time: tim, Valid: true}
	return nil
}

type TimetzCodec struct{}

func (TimetzCodec) FormatSupported(format int16) bool {
	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (TimetzCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (TimetzCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	if _, ok := value.(TimetzValuer); !ok {
		return nil
	}

	switch format {
	case pgtype.BinaryFormatCode:
		return encodePlanTimetzCodecBinary{}
	case pgtype.TextFormatCode:
		return encodePlanTimetzCodecText{}
	}

	return nil
}

type encodePlanTimetzCodecBinary struct{}

func (encodePlanTimetzCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimetzValuer).TimetzValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	base := time.Date(1970, 1, 1, 0, 0, 0, 0, ts.Time.Location())
	micros := ts.Time.Sub(base).Microseconds() % microsPerDay

	_, offset := ts.Time.Zone()

	buf = pgio.AppendInt64(buf, micros)
	buf = pgio.AppendInt32(buf, int32(offset))

	return buf, nil
}

type encodePlanTimetzCodecText struct{}

func (encodePlanTimetzCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimetzValuer).TimetzValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	s := ts.Time.Format(format)
	buf = append(buf, s...)

	return buf, nil
}

func (TimetzCodec) PlanScan(_ *pgtype.Map, _ uint32, format int16, target any) pgtype.ScanPlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case TimetzScanner:
			return scanPlanBinaryTimetzToTimetzScanner{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case TimetzScanner:
			return scanPlanTextTimetzToTimetzScanner{}
		}
	}

	return nil
}

type scanPlanBinaryTimetzToTimetzScanner struct{}

func (scanPlanBinaryTimetzToTimetzScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TimetzScanner)

	if src == nil {
		return scanner.ScanTimetz(Timetz{})
	}

	if len(src) != 12 {
		return fmt.Errorf("invalid length for timetz: %v", len(src))
	}

	micros := int64(binary.BigEndian.Uint64(src[:8]))
	offset := int32(binary.BigEndian.Uint32(src[8:]))

	nanos := micros*nanosPerMicros + int64(offset)*nanosPerSecond
	tim := time.Unix(0, nanos).In(time.UTC)

	return scanner.ScanTimetz(Timetz{Time: tim, Valid: true})
}

type scanPlanTextTimetzToTimetzScanner struct{}

func (scanPlanTextTimetzToTimetzScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TimetzScanner)

	if src == nil {
		return scanner.ScanTimetz(Timetz{})
	}

	sbuf := string(src)
	if sbuf[len(sbuf)-3] != ':' {
		sbuf = fmt.Sprintf("%s:00", sbuf)
	}

	var tim time.Time
	if strings.Contains(sbuf, "Z") {
		t, err := time.Parse(format, sbuf)
		if err != nil {
			return err
		}
		tim = t
	} else {
		t, err := time.Parse(secondFormat, sbuf)
		if err != nil {
			return err
		}
		tim = t
	}
	tim = tim.AddDate(1970, 0, 0).In(time.UTC)

	return scanner.ScanTimetz(Timetz{Time: tim, Valid: true})
}

func (c TimetzCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var ttz Timetz
	err := codecScan(c, m, oid, format, src, &ttz)
	if err != nil {
		return nil, err
	}
	return ttz, nil
}

func (c TimetzCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var ttz Timetz
	err := codecScan(c, m, oid, format, src, &ttz)
	if err != nil {
		return nil, err
	}
	return ttz, nil
}
