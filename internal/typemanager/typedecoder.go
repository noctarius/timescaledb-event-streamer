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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type typeDecoder func(src []byte) (any, error)

func findTypeDecoder(
	typeManager *typeManager, field pgconn.FieldDescription,
) (typeDecoder, error) {

	if pgType, ok := typeManager.typeMap.TypeForOID(field.DataTypeOID); ok {
		// Store a decoder wrapper for easier usage
		return asTypeDecoder(typeManager, pgType, field), nil
	}
	return nil, errors.Errorf("Unsupported type oid: %d", field.DataTypeOID)
}

func asTypeDecoder(
	typeManager *typeManager, pgType *pgtype.Type, field pgconn.FieldDescription,
) func(src []byte) (any, error) {

	return func(src []byte) (any, error) {
		return pgType.Codec.DecodeValue(typeManager.typeMap, field.DataTypeOID, field.Format, src)
	}
}
