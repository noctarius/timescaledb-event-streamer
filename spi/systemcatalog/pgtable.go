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
	"strings"
)

// PgTable represents a vanilla PostgreSQL table definition
// in the system catalog
type PgTable struct {
	*BaseTable
	relId uint32
}

// NewPgTable instantiates a new PgTable entity
func NewPgTable(
	relId uint32, schemaName, tableName string, replicaIdentity pgtypes.ReplicaIdentity,
) *PgTable {

	return &PgTable{
		BaseTable: newBaseTable(schemaName, tableName, replicaIdentity),
		relId:     relId,
	}
}

// RelId returns the table's relation id
func (t *PgTable) RelId() uint32 {
	return t.relId
}

// KeyIndexColumns returns a slice of ColumnAlike entries
// representing the snapshot index, or nil.
// A snapshot index must be the (composite) primary key of
// the table, since no dimensions exists (as with hypertables),
func (t *PgTable) KeyIndexColumns() []schema.ColumnAlike {
	index, present := (Columns(t.columns)).PrimaryKeyIndex()
	if !present {
		return nil
	}
	columns := make([]schema.ColumnAlike, 0, len(index.columns))
	for i := 0; i < len(index.columns); i++ {
		columns = append(columns, index.columns[i])
	}
	return columns
}

func (t *PgTable) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("relId:%d ", t.relId))
	builder.WriteString(fmt.Sprintf("schemaName:%s ", t.schemaName))
	builder.WriteString(fmt.Sprintf("tableName:%s ", t.tableName))
	builder.WriteString(fmt.Sprintf("replicaIdentity:%s", t.replicaIdentity))
	builder.WriteString("columns:[")
	for i, column := range t.columns {
		builder.WriteString(column.String())
		if i < len(t.columns)-1 {
			builder.WriteString(" ")
		}
	}
	builder.WriteString("]}")
	return builder.String()
}

// ApplyChanges applies catalog changes to a copy of the
// table (not updating the current one) and returns the
// new instance and a collection of applied changes.
func (t *PgTable) ApplyChanges(
	schemaName, tableName string, replicaIdentity pgtypes.ReplicaIdentity,
) (applied *PgTable, changes map[string]string) {

	t2 := &PgTable{
		BaseTable: t.BaseTable.ApplyChanges(schemaName, tableName, replicaIdentity),
		relId:     t.relId,
	}
	return t2, t.differences(t2)
}

func (t *PgTable) differences(
	new *PgTable,
) map[string]string {

	differences := make(map[string]string)
	if t.relId != new.relId {
		differences["relId"] = fmt.Sprintf("%d=>%d", t.relId, new.relId)
	}
	if t.schemaName != new.schemaName {
		differences["schemaName"] = fmt.Sprintf("%s=>%s", t.schemaName, new.schemaName)
	}
	if t.tableName != new.tableName {
		differences["hypertableName"] = fmt.Sprintf("%s=>%s", t.tableName, new.tableName)
	}
	if t.replicaIdentity != new.replicaIdentity {
		differences["replicaIdentity"] = fmt.Sprintf("%s=>%s", t.replicaIdentity, new.replicaIdentity)
	}
	return differences
}
