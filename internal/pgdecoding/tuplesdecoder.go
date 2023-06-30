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
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
)

func DecodeTuples(relation *pglogrepl.RelationMessage, tupleData *pglogrepl.TupleData) (map[string]any, error) {
	values := map[string]any{}
	if tupleData == nil {
		return values, nil
	}

	for idx, col := range tupleData.Columns {
		colName := relation.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and
			// logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text (basically anything other than the two above)
			val, err := DecodeTextColumn(col.Data, relation.Columns[idx].DataType)
			if err != nil {
				return nil, errors.Errorf("error decoding column data: %s", err)
			}
			values[colName] = val
		case 'b': // binary data
			val, err := DecodeBinaryColumn(col.Data, relation.Columns[idx].DataType)
			if err != nil {
				return nil, errors.Errorf("error decoding column data: %s", err)
			}
			values[colName] = val
		}
	}
	return values, nil
}
