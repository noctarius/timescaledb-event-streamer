package pgtypes

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
)

type LtreeScanner interface {
	ScanLtree(v Ltree) error
}

type LtreeValuer interface {
	LtreeValue() (Ltree, error)
}

type Ltree struct {
	Path  string
	Valid bool
}

func (l *Ltree) ScanLtree(v Ltree) error {
	*l = v
	return nil
}

func (l Ltree) LtreeValue() (Ltree, error) {
	return l, nil
}

func (l *Ltree) Scan(src any) error {
	if src == nil {
		*l = Ltree{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextLtreeToLtreeScanner{}.Scan([]byte(src), l)
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (l Ltree) Value() (driver.Value, error) {
	if !l.Valid {
		return nil, nil
	}

	return l.Path, nil
}

func (l Ltree) MarshalJSON() ([]byte, error) {
	if !l.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(l.Path)
}

func (l *Ltree) UnmarshalJSON(b []byte) error {
	var s *string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	if s == nil {
		*l = Ltree{}
		return nil
	}

	*l = Ltree{Path: *s, Valid: true}
	return nil
}

type LtreeCodec struct{}

func (LtreeCodec) FormatSupported(format int16) bool {
	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (LtreeCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (LtreeCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	if _, ok := value.(LtreeValuer); !ok {
		return nil
	}

	switch format {
	case pgtype.BinaryFormatCode:
		return encodePlanLtreeCodecBinary{}
	case pgtype.TextFormatCode:
		return encodePlanLtreeCodecText{}
	}

	return nil
}

type encodePlanLtreeCodecBinary struct{}

func (encodePlanLtreeCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ltree, err := value.(LtreeValuer).LtreeValue()
	if err != nil {
		return nil, err
	}

	if !ltree.Valid {
		return nil, nil
	}

	buf = append(buf, byte(1)) // version
	buf = append(buf, ltree.Path...)

	return buf, nil
}

type encodePlanLtreeCodecText struct{}

func (encodePlanLtreeCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ltree, err := value.(LtreeValuer).LtreeValue()
	if err != nil {
		return nil, err
	}

	if !ltree.Valid {
		return nil, nil
	}

	buf = append(buf, ltree.Path...)

	return buf, nil
}

func (LtreeCodec) PlanScan(_ *pgtype.Map, _ uint32, format int16, target any) pgtype.ScanPlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case LtreeScanner:
			return scanPlanBinaryLtreeToLtreeScanner{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case LtreeScanner:
			return scanPlanTextLtreeToLtreeScanner{}
		}
	}

	return nil
}

type scanPlanBinaryLtreeToLtreeScanner struct{}

func (scanPlanBinaryLtreeToLtreeScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(LtreeScanner)

	if src == nil {
		return scanner.ScanLtree(Ltree{})
	}

	if len(src) < 2 {
		return fmt.Errorf("invalid length for ltree: %v", len(src))
	}

	version := src[0]
	if version != 1 {
		return fmt.Errorf("unsupported version for ltree: %v", version)
	}

	return scanner.ScanLtree(Ltree{Path: string(src[1:]), Valid: true})
}

type scanPlanTextLtreeToLtreeScanner struct{}

func (scanPlanTextLtreeToLtreeScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(LtreeScanner)

	if src == nil {
		return scanner.ScanLtree(Ltree{})
	}

	return scanner.ScanLtree(Ltree{Path: string(src), Valid: true})
}

func (c LtreeCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var ltree Ltree
	err := codecScan(c, m, oid, format, src, &ltree)
	if err != nil {
		return nil, err
	}
	return ltree, nil
}

func (c LtreeCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var ltree Ltree
	err := codecScan(c, m, oid, format, src, &ltree)
	if err != nil {
		return nil, err
	}
	return ltree, nil
}
