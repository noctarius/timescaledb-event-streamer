package systemcatalog

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
)

// Columns represents a collection of columns which
// may or may not represent an index
type Columns []*Column

// HasPrimaryKey returns true if the collection of columns contains
// one or more primary key column(s)
func (c Columns) HasPrimaryKey() bool {
	return supporting.ContainsWithMatcher(c, func(other *Column) bool {
		return other.IsPrimaryKey()
	})
}

// PrimaryKeyIndex returns an Index instance which represents the
// primary key definition of the collection of columns and true,
// otherwise present will be false, meaning, that there isn't a
// primary key available in this collection
func (c Columns) PrimaryKeyIndex() (index *Index, present bool) {
	if !c.HasPrimaryKey() {
		return nil, false
	}

	primaryKeyColumns := supporting.Filter(c, func(item *Column) bool {
		return item.IsPrimaryKey()
	})

	supporting.Sort(primaryKeyColumns, func(this, other *Column) bool {
		return *this.keySeq < *other.keySeq
	})

	firstColumn := primaryKeyColumns[0]
	return newIndex(
		*firstColumn.indexName, primaryKeyColumns, true, false,
	), true
}

// HasReplicaIdentity returns true if the collection of columns contains
// one or more replica identity column(s)
func (c Columns) HasReplicaIdentity() bool {
	return supporting.ContainsWithMatcher(c, func(other *Column) bool {
		return other.IsReplicaIdent()
	})
}

// ReplicaIdentityIndex returns an Index instance which represents the
// primary key definition of the collection of columns and true,
// otherwise present will be false, meaning, that there isn't a
// primary key available in this collection
func (c Columns) ReplicaIdentityIndex() (index *Index, present bool) {
	if !c.HasReplicaIdentity() {
		return nil, false
	}

	replicaIdentityColumns := supporting.Filter(c, func(item *Column) bool {
		return item.IsReplicaIdent()
	})

	supporting.Sort(replicaIdentityColumns, func(this, other *Column) bool {
		return *this.keySeq < *other.keySeq
	})

	firstColumn := replicaIdentityColumns[0]
	return newIndex(
		*firstColumn.indexName, replicaIdentityColumns, false, true,
	), true
}

// Column represents a column from a hypertable or index
type Column struct {
	name         string
	dataType     uint32
	typeName     string
	nullable     bool
	primaryKey   bool
	keySeq       *int
	defaultValue *string
	replicaIdent bool
	indexName    *string
	sortOrder    IndexSortOrder
	nullsOrder   IndexNullsOrder
}

// NewColumn instantiates a new Column instance which isn't
// part of any index. This method is a shorthand version of
// NewIndexColumn
func NewColumn(name string, dataType uint32, typeName string, nullable bool, defaultValue *string) Column {
	return NewIndexColumn(
		name, dataType, typeName, nullable, false, nil,
		defaultValue, false, nil, ASC, NULLS_LAST,
	)
}

// NewIndexColumn instantiates a new Column instance
func NewIndexColumn(name string, dataType uint32, typeName string, nullable, primaryKey bool,
	keySeq *int, defaultValue *string, isReplicaIdent bool, indexName *string,
	sortOrder IndexSortOrder, nullsOrder IndexNullsOrder) Column {

	return Column{
		name:         name,
		dataType:     dataType,
		typeName:     typeName,
		nullable:     nullable,
		primaryKey:   primaryKey,
		keySeq:       keySeq,
		defaultValue: defaultValue,
		replicaIdent: isReplicaIdent,
		indexName:    indexName,
		sortOrder:    sortOrder,
		nullsOrder:   nullsOrder,
	}
}

// Name returns the column name
func (c Column) Name() string {
	return c.name
}

// DataType returns the PostgreSQL OID of the column
func (c Column) DataType() uint32 {
	return c.dataType
}

// TypeName returns the data type name of the column
func (c Column) TypeName() string {
	return c.typeName
}

// IsNullable returns true if the column is nullable
func (c Column) IsNullable() bool {
	return c.nullable
}

// IsPrimaryKey returns true if the columns is
// part of a primary key index
func (c Column) IsPrimaryKey() bool {
	return c.primaryKey
}

// IsReplicaIdent returns true if the columns is
// part of a replica identity index
func (c Column) IsReplicaIdent() bool {
	return c.replicaIdent
}

// DefaultValue returns the default value of the
// column, otherwise nil if no default value is
// defined
func (c Column) DefaultValue() *string {
	return c.defaultValue
}

func (c Column) equals(other Column) bool {
	return c.name == other.name &&
		c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		c.replicaIdent == other.replicaIdent &&
		((c.keySeq == nil && other.keySeq == nil) ||
			(c.keySeq != nil && other.keySeq != nil && *c.keySeq == *other.keySeq)) &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue)) &&
		((c.indexName == nil && other.indexName == nil) ||
			(c.indexName != nil && other.indexName != nil && *c.indexName == *other.indexName))
}

func (c Column) equalsExceptName(other Column) bool {
	return c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		c.replicaIdent == other.replicaIdent &&
		((c.keySeq == nil && other.keySeq == nil) ||
			(c.keySeq != nil && other.keySeq != nil && *c.keySeq == *other.keySeq)) &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue)) &&
		((c.indexName == nil && other.indexName == nil) ||
			(c.indexName != nil && other.indexName != nil && *c.indexName == *other.indexName))
}

func (c Column) differences(new Column) map[string]string {
	differences := make(map[string]string, 0)
	if c.name != new.name {
		differences["name"] = fmt.Sprintf("%s=>%s", c.name, new.name)
	}
	if c.dataType != new.dataType {
		differences["dataType"] = fmt.Sprintf("%d=>%d", c.dataType, new.dataType)
	}
	if c.typeName != new.typeName {
		differences["typeName"] = fmt.Sprintf("%s=>%s", c.typeName, new.typeName)
	}
	if c.nullable != new.nullable {
		differences["nullable"] = fmt.Sprintf("%t=>%t", c.nullable, new.nullable)
	}
	if c.primaryKey != new.primaryKey {
		differences["primaryKey"] = fmt.Sprintf("%t=>%t", c.primaryKey, new.primaryKey)
	}
	if (c.keySeq == nil && new.keySeq != nil) ||
		(c.keySeq != nil && new.keySeq == nil) {
		o := "<nil>"
		if c.keySeq != nil {
			o = fmt.Sprintf("%d", *c.keySeq)
		}
		n := "<nil>"
		if new.keySeq != nil {
			n = fmt.Sprintf("%d", *new.keySeq)
		}
		differences["keySeq"] = fmt.Sprintf("%s=>%s", o, n)
	}
	if (c.indexName == nil && new.indexName != nil) ||
		(c.indexName != nil && new.indexName == nil) {
		o := "<nil>"
		if c.indexName != nil {
			o = *c.indexName
		}
		n := "<nil>"
		if new.indexName != nil {
			n = *new.indexName
		}
		differences["indexName"] = fmt.Sprintf("%s=>%s", o, n)
	}
	if c.replicaIdent != new.replicaIdent {
		differences["replicaIdent"] = fmt.Sprintf("%t=>%t", c.replicaIdent, new.replicaIdent)
	}
	if (c.defaultValue == nil && new.defaultValue != nil) ||
		(c.defaultValue != nil && new.defaultValue == nil) {
		o := "<nil>"
		if c.defaultValue != nil {
			o = *c.defaultValue
		}
		n := "<nil>"
		if new.defaultValue != nil {
			n = *new.defaultValue
		}
		differences["defaultValue"] = fmt.Sprintf("%s=>%s", o, n)
	}
	return differences
}
