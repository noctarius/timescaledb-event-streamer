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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type rowDecoder struct {
	decoders []func(src []byte) (any, error)
	fields   []pgconn.FieldDescription
}

func newRowDecoder(
	typeManager *typeManager, fields []pgconn.FieldDescription,
) (*rowDecoder, error) {

	decoders := make([]func(src []byte) (any, error), 0)
	for _, field := range fields {
		if decoder, err := findTypeDecoder(typeManager, field); err != nil {
			return nil, errors.Wrap(err, 0)
		} else {
			// Store a decoder wrapper for easier usage
			decoders = append(decoders, decoder)
		}
	}
	return &rowDecoder{
		decoders: decoders,
		fields:   fields,
	}, nil
}

func (rd *rowDecoder) DecodeRowsMapAndSink(
	rows pgx.Rows, sink func(values map[string]any) error,
) error {

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

func (rd *rowDecoder) DecodeRowsAndSink(
	rows pgx.Rows, sink func(values []any) error,
) error {

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

func (rd *rowDecoder) Decode(
	rawRow [][]byte,
) ([]any, error) {

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

func (rd *rowDecoder) DecodeAndSink(
	rawRow [][]byte, sink func(values []any) error,
) error {

	if values, err := rd.Decode(rawRow); err != nil {
		return errors.Wrap(err, 0)
	} else {
		return sink(values)
	}
}

func (rd *rowDecoder) DecodeMapAndSink(
	rawRow [][]byte, sink func(values map[string]any) error,
) error {

	if values, err := rd.Decode(rawRow); err != nil {
		return errors.Wrap(err, 0)
	} else {
		resultSet := make(map[string]any)
		for i, field := range rd.fields {
			resultSet[field.Name] = values[i]
		}
		if err := sink(resultSet); err != nil {
			return errors.Wrap(err, 0)
		}
	}
	return nil
}

func (rd *rowDecoder) compatible(
	other []pgconn.FieldDescription,
) bool {

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
		// if f.TableAttributeNumber != o.TableAttributeNumber { return false }
		// if f.TableOID != o.TableOID { return false }
	}
	return true
}
