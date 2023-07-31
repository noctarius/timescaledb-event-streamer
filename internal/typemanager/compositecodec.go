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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
)

func newCompositeCodec(
	typeManager *typeManager, typ pgtypes.PgType,
) (*pgtype.CompositeCodec, error) {

	columns, err := typ.CompositeColumns()
	if err != nil {
		panic(err)
	}

	compositeCodecColumns := make([]pgtype.CompositeCodecField, len(columns))
	for i, column := range columns {
		pgType, present := typeManager.typeMap.TypeForOID(column.Type().Oid())
		if !present {
			return nil, errors.Errorf(
				"Required type %d for column %s not found in type map", column.Type().Oid(), column.Name(),
			)
		}

		compositeCodecColumns[i] = pgtype.CompositeCodecField{
			Name: column.Name(),
			Type: pgType,
		}
	}
	return &pgtype.CompositeCodec{
		Fields: compositeCodecColumns,
	}, nil
}
