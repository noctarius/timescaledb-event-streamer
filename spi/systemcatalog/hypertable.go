package systemcatalog

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
)

// Hypertable represents a TimescaleDB hypertable definition
// in the system catalog
type Hypertable struct {
	*baseSystemEntity
	id                     int32
	databaseName           string
	associatedSchemaName   string
	associatedTablePrefix  string
	compressedHypertableId *int32
	compressionState       int16
	distributed            bool
	continuousAggregate    bool
	viewSchema             *string
	viewName               *string
	columns                []Column
	replicaIdentity        pgtypes.ReplicaIdentity
}

// NewHypertable instantiates a new Hypertable entity
func NewHypertable(id int32,
	databaseName, schemaName, tableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16, distributed bool,
	viewSchema, viewName *string, replicaIdentity pgtypes.ReplicaIdentity) *Hypertable {

	return &Hypertable{
		baseSystemEntity: &baseSystemEntity{
			schemaName: schemaName,
			tableName:  tableName,
		},
		id:                     id,
		databaseName:           databaseName,
		associatedSchemaName:   associatedSchemaName,
		associatedTablePrefix:  associatedTablePrefix,
		compressedHypertableId: compressedHypertableId,
		compressionState:       compressionState,
		distributed:            distributed,
		continuousAggregate:    isContinuousAggregate(tableName, viewSchema, viewName),
		viewSchema:             viewSchema,
		viewName:               viewName,
		replicaIdentity:        replicaIdentity,
		columns:                make([]Column, 0),
	}
}

// Id returns the hypertable id
func (h *Hypertable) Id() int32 {
	return h.id
}

// DatabaseName returns the database name
func (h *Hypertable) DatabaseName() string {
	return h.databaseName
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

// Columns returns a slice with the column definitions
// of the hypertable
func (h *Hypertable) Columns() Columns {
	return h.columns
}

// CanonicalContinuousAggregateName returns the canonical
// continuous aggregate name of the hypertable in the form
// of <<schema.view>>. This method panics if the hypertable
// doesn't back a continuous aggregate. A check using
// IsContinuousAggregate before calling this method is adviced.
func (h *Hypertable) CanonicalContinuousAggregateName() string {
	return canonicalContinuousAggregateName(h)
}

// ReplicaIdentity returns the replica identity (if available),
// otherwise a pgtypes.UNKNOWN is returned
func (h *Hypertable) ReplicaIdentity() pgtypes.ReplicaIdentity {
	return h.replicaIdentity
}

// ApplyTableSchema applies a new hypertable schema to this
// hypertable instance and returns changes to the previously
// known schema layout.
func (h *Hypertable) ApplyTableSchema(newColumns []Column) (changes map[string]string) {
	oldColumns := h.columns
	h.columns = newColumns

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
				for o := i; o < min(len(oldColumns), len(newColumns)); o++ {
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

// ApplyChanges applies catalog changes to a copy of the
// hypertable instance (not updating the current one) and
// returns the new instance and a collection of applied
// changes.
func (h *Hypertable) ApplyChanges(
	schemaName, tableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16,
	replicaIdentity pgtypes.ReplicaIdentity) (applied *Hypertable, changes map[string]string) {

	h2 := &Hypertable{
		baseSystemEntity: &baseSystemEntity{
			schemaName: schemaName,
			tableName:  tableName,
		},
		id:                     h.id,
		associatedSchemaName:   associatedSchemaName,
		associatedTablePrefix:  associatedTablePrefix,
		compressedHypertableId: compressedHypertableId,
		compressionState:       compressionState,
		distributed:            h.distributed,
		columns:                h.columns,
		databaseName:           h.databaseName,
		replicaIdentity:        replicaIdentity,
	}
	return h2, h.differences(h2)
}

func (h *Hypertable) differences(new *Hypertable) map[string]string {
	differences := make(map[string]string, 0)
	if h.id != new.id {
		differences["id"] = fmt.Sprintf("%d=>%d", h.id, new.id)
	}
	if h.schemaName != new.schemaName {
		differences["schemaName"] = fmt.Sprintf("%s=>%s", h.schemaName, new.schemaName)
	}
	if h.tableName != new.tableName {
		differences["hypertableName"] = fmt.Sprintf("%s=>%s", h.tableName, new.tableName)
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
	if h.replicaIdentity != new.replicaIdentity {
		differences["replicaIdentity"] = fmt.Sprintf("%s=>%s", h.replicaIdentity, new.replicaIdentity)
	}
	return differences
}
