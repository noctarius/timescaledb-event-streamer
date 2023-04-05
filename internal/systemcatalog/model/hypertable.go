package model

import (
	"fmt"
)

type Hypertable struct {
	id                     int32
	databaseName           string
	schemaName             string
	hypertableName         string
	associatedSchemaName   string
	associatedTablePrefix  string
	compressedHypertableId *int32
	compressionState       int16
	distributed            bool
	continuousAggregate    bool
	viewSchema             *string
	viewName               *string
	columns                []Column
}

func NewHypertable(id int32,
	databaseName, schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32, compressionState int16, distributed bool, viewSchema, viewName *string) *Hypertable {

	return &Hypertable{
		id:                     id,
		databaseName:           databaseName,
		schemaName:             schemaName,
		hypertableName:         hypertableName,
		associatedSchemaName:   associatedSchemaName,
		associatedTablePrefix:  associatedTablePrefix,
		compressedHypertableId: compressedHypertableId,
		compressionState:       compressionState,
		distributed:            distributed,
		continuousAggregate:    isContinuousAggregate(hypertableName, viewSchema, viewName),
		viewSchema:             viewSchema,
		viewName:               viewName,
		columns:                make([]Column, 0),
	}
}

func (h *Hypertable) Id() int32 {
	return h.id
}

func (h *Hypertable) DatabaseName() string {
	return h.databaseName
}

func (h *Hypertable) SchemaName() string {
	return h.schemaName
}

func (h *Hypertable) HypertableName() string {
	return h.hypertableName
}

func (h *Hypertable) ViewSchema() (string, bool) {
	if h.viewSchema != nil {
		return *h.viewSchema, true
	}
	return "", false
}

func (h *Hypertable) ViewName() (string, bool) {
	if h.viewName != nil {
		return *h.viewName, true
	}
	return "", false
}

func (h *Hypertable) AssociatedSchemaName() string {
	return h.associatedSchemaName
}

func (h *Hypertable) AssociatedTablePrefix() string {
	return h.associatedTablePrefix
}

func (h *Hypertable) CompressedHypertableId() (int32, bool) {
	if h.compressedHypertableId != nil {
		return *h.compressedHypertableId, true
	}
	return 0, false
}

func (h *Hypertable) IsCompressionEnabled() bool {
	return h.compressionState == 1
}

func (h *Hypertable) IsCompressedTable() bool {
	return h.compressionState == 2
}

func (h *Hypertable) IsDistributed() bool {
	return h.distributed
}

func (h *Hypertable) IsContinuousAggregate() bool {
	return h.continuousAggregate
}

func (h *Hypertable) Columns() []Column {
	return h.columns
}

func (h *Hypertable) CanonicalName() string {
	return canonicalHypertableName(h)
}

func (h *Hypertable) CanonicalContinuousAggregateName() string {
	return canonicalContinuousAggregateName(h)
}

func (h *Hypertable) CanonicalChunkTablePrefix() string {
	return canonicalChunkTablePrefix(h)
}

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

func (h *Hypertable) ApplyChanges(
	schemaName, hypertableName, associatedSchemaName, associatedTablePrefix string,
	compressedHypertableId *int32,
	compressionState int16) (applied *Hypertable, changes map[string]string) {

	h2 := &Hypertable{
		id:                     h.id,
		schemaName:             schemaName,
		hypertableName:         hypertableName,
		associatedSchemaName:   associatedSchemaName,
		associatedTablePrefix:  associatedTablePrefix,
		compressedHypertableId: compressedHypertableId,
		compressionState:       compressionState,
		distributed:            h.distributed,
		columns:                h.columns,
		databaseName:           h.databaseName,
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
	if h.hypertableName != new.hypertableName {
		differences["hypertableName"] = fmt.Sprintf("%s=>%s", h.hypertableName, new.hypertableName)
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
	return differences
}
