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

package pgtypes

import "github.com/jackc/pgx/v5/pgtype"

func AsFieldLength(
	typ PgType, modifiers int,
) (int, bool) {

	switch typ.Oid() {
	case pgtype.BitOID, pgtype.VarbitOID, pgtype.BitArrayOID, pgtype.VarbitArrayOID:
		if modifiers > 1 {
			return modifiers, true
		}

	case pgtype.BPCharOID, pgtype.VarcharOID, pgtype.BPCharArrayOID, pgtype.VarcharArrayOID:
		if modifiers > 5 {
			return modifiers - 4, true
		}
	}
	return -1, false
}
