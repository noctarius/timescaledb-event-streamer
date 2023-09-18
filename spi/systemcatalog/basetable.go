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

package systemcatalog

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/samber/lo"
)

type BaseTable struct {
	*baseSystemEntity
	tableColumns    []schema.ColumnAlike
	replicaIdentity pgtypes.ReplicaIdentity
	columns         []Column
}

func newBaseTable(
	schemaName, tableName string, replicaIdentity pgtypes.ReplicaIdentity,
) *BaseTable {

	return &BaseTable{
		baseSystemEntity: &baseSystemEntity{
			schemaName: schemaName,
			tableName:  tableName,
		},
		replicaIdentity: replicaIdentity,
		columns:         make([]Column, 0),
		tableColumns:    make([]schema.ColumnAlike, 0),
	}
}

// Columns returns a slice with the column definitions
// of the table
func (bt *BaseTable) Columns() Columns {
	return bt.columns
}

// TableColumns returns a slice of ColumnAlike entries
// representing the columns of this table
func (bt *BaseTable) TableColumns() []schema.ColumnAlike {
	return bt.tableColumns
}

// ReplicaIdentity returns the replica identity (if available),
// otherwise a pgtypes.UNKNOWN is returned
func (bt *BaseTable) ReplicaIdentity() pgtypes.ReplicaIdentity {
	return bt.replicaIdentity
}

// SchemaBuilder returns a SchemaBuilder instance, preconfigured
// for this table
func (bt *BaseTable) SchemaBuilder() schema.Builder {
	schemaBuilder := schema.NewSchemaBuilder(schema.STRUCT).
		FieldName(bt.CanonicalName())

	for i, column := range bt.columns {
		schemaBuilder.Field(column.Name(), i, column.SchemaBuilder())
	}
	return schemaBuilder
}

// ApplyTableSchema applies a new table schema to this
// table and returns changes to the previously
// known schema layout.
func (bt *BaseTable) ApplyTableSchema(
	newColumns []Column,
) (changes map[string]string) {

	oldColumns := bt.columns
	bt.columns = newColumns

	bt.tableColumns = make([]schema.ColumnAlike, 0, len(newColumns))
	for i := 0; i < len(newColumns); i++ {
		bt.tableColumns = append(bt.tableColumns, newColumns[i])
	}

	newIndex := 0
	differences := make(map[string]string, 0)
	for i, c1 := range oldColumns {
		// dropped last column
		if len(newColumns) <= newIndex {
			differences[c1.Name()] = fmt.Sprintf("dropped: %+v", c1)
			continue
		}

		c2 := newColumns[newIndex]
		if c1.equals(c2) {
			newIndex++
			continue
		}

		handled := false
		if c1.equalsExceptName(c2) {
			// seems like last column was renamed
			if newIndex+1 == len(newColumns) {
				differences[c1.Name()] = fmt.Sprintf("name:%s=>%s", c1.Name(), c2.Name())
				handled = true
				if i < len(newColumns) {
					newIndex++
				}
			} else {
				// potentially renamed, run look ahead
				lookAheadSuccessful := false
				for o := i; o < lo.Min([]int{len(oldColumns), len(newColumns)}); o++ {
					if oldColumns[o].equals(newColumns[o]) {
						lookAheadSuccessful = true
					}
				}

				if lookAheadSuccessful {
					differences[c2.Name()] = fmt.Sprintf("name:%s=>%s", c1.Name(), c2.Name())
					handled = true
					newIndex++
				}
			}
		}
		if len(oldColumns) > i+1 && oldColumns[i+1].equals(c2) {
			differences[c1.Name()] = fmt.Sprintf("dropped: %+v", c1)
			handled = true
		}

		if !handled {
			differences[c1.Name()] = fmt.Sprintf("%+v", c1.differences(c2))
			newIndex++
		}
	}
	for i := newIndex; i < len(newColumns); i++ {
		c := newColumns[i]
		differences[c.Name()] = fmt.Sprintf("added: %+v", c)
	}
	return differences
}

func (bt *BaseTable) ApplyChanges(
	schemaName, tableName string, replicaIdentity pgtypes.ReplicaIdentity,
) *BaseTable {

	return &BaseTable{
		baseSystemEntity: &baseSystemEntity{
			schemaName: schemaName,
			tableName:  tableName,
		},
		replicaIdentity: replicaIdentity,
		columns:         bt.columns,
		tableColumns:    bt.tableColumns,
	}
}
