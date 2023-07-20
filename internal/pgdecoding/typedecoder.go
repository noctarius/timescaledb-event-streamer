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

package pgdecoding

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var typeMap *pgtype.Map

func init() {
	typeMap = pgtype.NewMap()

	macaddr8Type := &pgtype.Type{Name: "macaddr8", OID: 774, Codec: pgtype.MacaddrCodec{}}
	typeMap.RegisterType(macaddr8Type)
	typeMap.RegisterType(
		&pgtype.Type{Name: "_macaddr8", OID: 775, Codec: &pgtype.ArrayCodec{ElementType: macaddr8Type}},
	)

	xmlType := &pgtype.Type{Name: "xml", OID: 142, Codec: XmlCodec{}}
	typeMap.RegisterType(xmlType)
	typeMap.RegisterType(
		&pgtype.Type{Name: "_xml", OID: 143, Codec: &pgtype.ArrayCodec{ElementType: xmlType}},
	)

	timetzType := &pgtype.Type{Name: "timetz", OID: 1266, Codec: &TimetzCodec{}}
	typeMap.RegisterType(timetzType)
	typeMap.RegisterType(
		&pgtype.Type{Name: "_timetz", OID: 1270, Codec: &pgtype.ArrayCodec{ElementType: timetzType}},
	)

	qcharType, _ := typeMap.TypeForOID(pgtype.QCharOID)
	typeMap.RegisterType(
		&pgtype.Type{Name: "_char", OID: 1002, Codec: &pgtype.ArrayCodec{ElementType: qcharType}},
	)
}

type RowDecoder struct {
	decoders []func(src []byte) (any, error)
	fields   []pgconn.FieldDescription
}

func NewRowDecoder(fields []pgconn.FieldDescription) (*RowDecoder, error) {
	decoders := make([]func(src []byte) (any, error), 0)
	for _, field := range fields {
		if decoder, err := FindTypeDecoder(field); err != nil {
			return nil, errors.Wrap(err, 0)
		} else {
			// Store a decoder wrapper for easier usage
			decoders = append(decoders, decoder)
		}
	}
	return &RowDecoder{
		decoders: decoders,
		fields:   fields,
	}, nil
}

func (rd *RowDecoder) DecodeRowsMapAndSink(rows pgx.Rows, sink func(values map[string]any) error) error {
	if !rd.compatible(rows.FieldDescriptions()) {
		return errors.Errorf("incompatible rows instance provided")
	}

	// Initial error check
	if rows.Err() != nil {
		return errors.Wrap(rows.Err(), 0)
	}
	defer rows.Close()

	for rows.Next() {
		values, err := rd.Decode(rows.RawValues())
		if err != nil {
			return errors.Wrap(err, 0)
		}

		resultSet := make(map[string]any, 0)
		for i, field := range rd.fields {
			resultSet[field.Name] = values[i]
		}
		if err := sink(resultSet); err != nil {
			return errors.Wrap(err, 0)
		}
	}
	if rows.Err() != nil {
		return errors.Wrap(rows.Err(), 0)
	}
	return nil
}

func (rd *RowDecoder) DecodeRowsAndSink(rows pgx.Rows, sink func(values []any) error) error {
	if !rd.compatible(rows.FieldDescriptions()) {
		return errors.Errorf("incompatible rows instance provided")
	}

	// Initial error check
	if rows.Err() != nil {
		return errors.Wrap(rows.Err(), 0)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rd.DecodeAndSink(rows.RawValues(), sink); err != nil {
			return errors.Wrap(err, 0)
		}
	}
	if rows.Err() != nil {
		return errors.Wrap(rows.Err(), 0)
	}
	return nil
}

func (rd *RowDecoder) Decode(rawRow [][]byte) ([]any, error) {
	values := make([]any, 0)
	for i, decoder := range rd.decoders {
		if v, err := decoder(rawRow[i]); err != nil {
			return nil, errors.Wrap(err, 0)
		} else {
			values = append(values, v)
		}
	}
	return values, nil
}

func (rd *RowDecoder) DecodeAndSink(rawRow [][]byte, sink func(values []any) error) error {
	if values, err := rd.Decode(rawRow); err != nil {
		return errors.Wrap(err, 0)
	} else {
		return sink(values)
	}
}

func (rd *RowDecoder) DecodeMapAndSink(rawRow [][]byte, sink func(values map[string]any) error) error {
	if values, err := rd.Decode(rawRow); err != nil {
		return errors.Wrap(err, 0)
	} else {
		resultSet := make(map[string]any, 0)
		for i, field := range rd.fields {
			resultSet[field.Name] = values[i]
		}
		if err := sink(resultSet); err != nil {
			return errors.Wrap(err, 0)
		}
	}
	return nil
}

func (rd *RowDecoder) compatible(other []pgconn.FieldDescription) bool {
	if len(rd.fields) != len(other) {
		return false
	}

	for i, f := range rd.fields {
		o := other[i]
		if f.Format != o.Format {
			return false
		}
		if f.DataTypeOID != o.DataTypeOID {
			return false
		}
		if f.Name != o.Name {
			return false
		}
		if f.DataTypeSize != o.DataTypeSize {
			return false
		}
		if f.TypeModifier != o.TypeModifier {
			return false
		}
		// Can we reuse the same decoder for all chunks? 🤔
		//if f.TableAttributeNumber != o.TableAttributeNumber { return false }
		// if f.TableOID != o.TableOID { return false }
	}
	return true
}

func DecodeTextColumn(src []byte, dataTypeOid uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataTypeOid); ok {
		return dt.Codec.DecodeValue(typeMap, dataTypeOid, pgtype.TextFormatCode, src)
	}
	return string(src), nil
}

func DecodeBinaryColumn(src []byte, dataTypeOid uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataTypeOid); ok {
		return dt.Codec.DecodeValue(typeMap, dataTypeOid, pgtype.BinaryFormatCode, src)
	}
	return string(src), nil
}

func DecodeValue(field pgconn.FieldDescription, src []byte) (any, error) {
	if t, ok := typeMap.TypeForOID(field.DataTypeOID); ok {
		return t.Codec.DecodeValue(typeMap, field.DataTypeOID, field.Format, src)
	}
	return nil, errors.Errorf("Unsupported type oid: %d", field.DataTypeOID)
}

func DecodeRowValues(rows pgx.Rows, sink func(values []any) error) error {
	decoder, err := NewRowDecoder(rows.FieldDescriptions())
	if err != nil {
		return err
	}
	return decoder.DecodeRowsAndSink(rows, sink)
}

func FindTypeDecoder(field pgconn.FieldDescription) (func(src []byte) (any, error), error) {
	if t, ok := typeMap.TypeForOID(field.DataTypeOID); ok {
		// Store a decoder wrapper for easier usage
		return asTypeDecoder(t, field), nil
	}
	return nil, errors.Errorf("Unsupported type oid: %d", field.DataTypeOID)
}

func RegisterType(t *pgtype.Type) {
	typeMap.RegisterType(t)
}

func GetType(oid uint32) (*pgtype.Type, bool) {
	return typeMap.TypeForOID(oid)
}

func asTypeDecoder(t *pgtype.Type, field pgconn.FieldDescription) func(src []byte) (any, error) {
	return func(src []byte) (any, error) {
		return t.Codec.DecodeValue(typeMap, field.DataTypeOID, field.Format, src)
	}
}

func codecScan(codec pgtype.Codec, m *pgtype.Map, oid uint32, format int16, src []byte, dst any) error {
	scanPlan := codec.PlanScan(m, oid, format, dst)
	if scanPlan == nil {
		return fmt.Errorf("PlanScan did not find a plan")
	}
	return scanPlan.Scan(src, dst)
}
