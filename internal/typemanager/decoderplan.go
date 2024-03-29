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
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/functional"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
)

type tupleDecoder func(column *pglogrepl.TupleDataColumn, values map[string]any) error

type tupleCodec func(data []byte, binary bool) (any, error)

func planTupleDecoder(
	typeManager *typeManager, relation *pgtypes.RelationMessage,
) (pgtypes.TupleDecoderPlan, error) {

	decoders := make([]tupleDecoder, 0)

	for _, column := range relation.Columns {
		codec := func(data []byte, binary bool) (any, error) {
			return string(data), nil
		}
		if pgxType, ok := typeManager.typeMap.TypeForOID(column.DataType); ok {
			codec = func(data []byte, binary bool) (any, error) {
				dataformat := int16(pgtype.TextFormatCode)
				if binary {
					dataformat = pgtype.BinaryFormatCode
				}
				return pgxType.Codec.DecodeValue(typeManager.typeMap, column.DataType, dataformat, data)
			}
		}

		decoders = append(decoders, func(dataType uint32, name string, codec tupleCodec) tupleDecoder {
			return func(column *pglogrepl.TupleDataColumn, values map[string]any) error {
				switch column.DataType {
				case 'n': // null
					values[name] = nil
				case 'u': // unchanged toast
					// This TOAST value was not changed. TOAST values are not stored in the tuple, and
					// logical replication doesn't want to spend a disk read to fetch its value for you.
				case 't': // text (basically anything other than the two above)
					val, err := codec(column.Data, false)
					if err != nil {
						return errors.Errorf("error decoding column data: %s", err)
					}
					values[name] = val
				case 'b': // binary data
					val, err := codec(column.Data, true)
					if err != nil {
						return errors.Errorf("error decoding column data: %s", err)
					}
					values[name] = val
				}
				return nil
			}
		}(column.DataType, column.Name, codec))
	}

	return &tupleDecoderPlan{
		decoders: decoders,
	}, nil
}

type tupleDecoderPlan struct {
	decoders []tupleDecoder
}

func (tdp *tupleDecoderPlan) Decode(
	tupleData *pglogrepl.TupleData,
) (map[string]any, error) {

	if tupleData == nil {
		return functional.Zero[map[string]any](), nil
	}

	values := map[string]any{}
	for i, decoder := range tdp.decoders {
		column := tupleData.Columns[i]
		if err := decoder(column, values); err != nil {
			return nil, err
		}
	}
	return values, nil
}
