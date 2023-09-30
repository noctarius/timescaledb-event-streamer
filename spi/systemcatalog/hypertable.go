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

// Hypertable represents a TimescaleDB hypertable definition
// in the system catalog
type Hypertable struct {
	*baseTable
	id                     int32
	associatedSchemaName   string
	associatedTablePrefix  string
	compressedHypertableId *int32
	compressionState       int16
	distributed            bool
	continuousAggregate    bool
	viewSchema             *string
	viewName               *string
}

// NewHypertable instantiates a new Hypertable entity
func NewHypertable(
	id int32, schemaName, tableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16, distributed bool,
	viewSchema, viewName *string, replicaIdentity pgtypes.ReplicaIdentity,
) *Hypertable {

	return &Hypertable{
		baseTable:              newBaseTable(schemaName, tableName, replicaIdentity),
		id:                     id,
		associatedSchemaName:   associatedSchemaName,
		associatedTablePrefix:  associatedTablePrefix,
		compressedHypertableId: compressedHypertableId,
		compressionState:       compressionState,
		distributed:            distributed,
		continuousAggregate:    isContinuousAggregate(tableName, viewSchema, viewName),
		viewSchema:             viewSchema,
		viewName:               viewName,
	}
}

// Id returns the hypertable id
func (h *Hypertable) Id() int32 {
	return h.id
}

// ViewSchema returns the view schema name and true if the
// hypertable is a backing hypertable for a continuous aggregate,
// otherwise the present will be false
func (h *Hypertable) ViewSchema() (viewSchemaName string, present bool) {
	if h.viewSchema != nil {
		return *h.viewSchema, true
	}
	return "", false
}

// ViewName returns the view name and true if the hypertable
// is a backing hypertable for a continuous aggregate,
// otherwise the present will be false
func (h *Hypertable) ViewName() (viewName string, present bool) {
	if h.viewName != nil {
		return *h.viewName, true
	}
	return "", false
}

// CompressedHypertableId returns the id of a hypertable which
// is used to represent the compressed chunks and true, otherwise
// present will be false
func (h *Hypertable) CompressedHypertableId() (compressedHypertableId int32, present bool) {
	if h.compressedHypertableId != nil {
		return *h.compressedHypertableId, true
	}
	return 0, false
}

// IsCompressionEnabled returns true if the hypertable has
// compression enabled (not if the hypertable is a compressed
// hypertable), otherwise false
func (h *Hypertable) IsCompressionEnabled() bool {
	return h.compressionState == 1
}

// IsCompressedTable returns true if the hypertable is a
// compressed hypertable (not if the hypertable has compression
// enabled), otherwise false
func (h *Hypertable) IsCompressedTable() bool {
	return h.compressionState == 2
}

// IsDistributed returns true if the hypertable is a
// distributed hypertable, otherwise false
func (h *Hypertable) IsDistributed() bool {
	return h.distributed
}

// IsContinuousAggregate returns true if the hypertable
// is a backing hypertable for a continues aggregate,
// otherwise false
func (h *Hypertable) IsContinuousAggregate() bool {
	return h.continuousAggregate
}

// KeyIndexColumns returns a slice of ColumnAlike entries
// representing the snapshot index, or nil.
// A snapshot index is either the (composite) primary key or
// a "virtual" index built from the hypertable's dimensions
func (h *Hypertable) KeyIndexColumns() []schema.ColumnAlike {
	index, present := (h.Columns()).SnapshotIndex()
	if !present {
		return nil
	}
	columns := make([]schema.ColumnAlike, 0, len(index.columns))
	for i := 0; i < len(index.columns); i++ {
		columns = append(columns, index.columns[i])
	}
	return columns
}

// CanonicalContinuousAggregateName returns the canonical
// continuous aggregate name of the hypertable in the form
// of <<schema.view>>. This method panics if the hypertable
// doesn't back a continuous aggregate. A check using
// IsContinuousAggregate before calling this method is advised.
func (h *Hypertable) CanonicalContinuousAggregateName() string {
	return canonicalContinuousAggregateName(h)
}

func (h *Hypertable) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("id:%d ", h.id))
	builder.WriteString(fmt.Sprintf("schemaName:%s ", h.SchemaName()))
	builder.WriteString(fmt.Sprintf("tableName:%s ", h.TableName()))
	builder.WriteString(fmt.Sprintf("associatedSchemaName:%s ", h.associatedSchemaName))
	builder.WriteString(fmt.Sprintf("associatedTablePrefix:%s ", h.associatedTablePrefix))
	if h.compressedHypertableId == nil {
		builder.WriteString("compressedHypertableId:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("compressedHypertableId:%d ", *h.compressedHypertableId))
	}
	builder.WriteString(fmt.Sprintf("compressionState:%d ", h.compressionState))
	builder.WriteString(fmt.Sprintf("replicaIdentity:%s", h.ReplicaIdentity()))
	if h.viewSchema == nil {
		builder.WriteString("viewSchema:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("viewSchema:%s ", *h.viewSchema))
	}
	if h.viewName == nil {
		builder.WriteString("viewName:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("viewName:%s ", *h.viewName))
	}
	builder.WriteString("columns:[")
	for i, column := range h.Columns() {
		builder.WriteString(column.String())
		if i < len(h.Columns())-1 {
			builder.WriteString(" ")
		}
	}
	builder.WriteString("]}")
	return builder.String()
}

// ApplyChanges applies catalog changes to a copy of the
// hypertable instance (not updating the current one) and
// returns the new instance and a collection of applied
// changes.
func (h *Hypertable) ApplyChanges(
	schemaName, tableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16,
	replicaIdentity pgtypes.ReplicaIdentity,
) (applied *Hypertable, changes map[string]string) {

	h2 := &Hypertable{
		baseTable:              h.baseTable.ApplyChanges(schemaName, tableName, replicaIdentity),
		id:                     h.id,
		associatedSchemaName:   associatedSchemaName,
		associatedTablePrefix:  associatedTablePrefix,
		compressedHypertableId: compressedHypertableId,
		compressionState:       compressionState,
		distributed:            h.distributed,
	}
	return h2, h.differences(h2)
}

func (h *Hypertable) differences(
	new *Hypertable,
) map[string]string {

	differences := make(map[string]string)
	if h.id != new.id {
		differences["id"] = fmt.Sprintf("%d=>%d", h.id, new.id)
	}
	if h.SchemaName() != new.SchemaName() {
		differences["schemaName"] = fmt.Sprintf("%s=>%s", h.SchemaName(), new.SchemaName())
	}
	if h.TableName() != new.TableName() {
		differences["hypertableName"] = fmt.Sprintf("%s=>%s", h.TableName(), new.TableName())
	}
	if h.associatedSchemaName != new.associatedSchemaName {
		differences["associatedSchemaName"] = fmt.Sprintf("%s=>%s", h.associatedSchemaName, new.associatedSchemaName)
	}
	if h.associatedTablePrefix != new.associatedTablePrefix {
		differences["associatedTablePrefix"] = fmt.Sprintf("%s=>%s", h.associatedTablePrefix, new.associatedTablePrefix)
	}
	if (h.compressedHypertableId == nil && new.compressedHypertableId != nil) ||
		(h.compressedHypertableId != nil && new.compressedHypertableId == nil) {
		o := "<nil>"
		if h.compressedHypertableId != nil {
			o = fmt.Sprintf("%d", *h.compressedHypertableId)
		}
		n := "<nil>"
		if new.compressedHypertableId != nil {
			n = fmt.Sprintf("%d", *new.compressedHypertableId)
		}
		differences["compressedHypertableId"] = fmt.Sprintf("%s=>%s", o, n)
	}
	if h.compressionState != new.compressionState {
		differences["compressionState"] = fmt.Sprintf("%d=>%d", h.compressionState, new.compressionState)
	}
	if h.ReplicaIdentity() != new.ReplicaIdentity() {
		differences["replicaIdentity"] = fmt.Sprintf("%s=>%s", h.ReplicaIdentity(), new.ReplicaIdentity())
	}
	return differences
}
