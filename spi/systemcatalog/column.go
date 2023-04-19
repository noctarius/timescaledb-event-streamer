package systemcatalog

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
)

type Columns []*Column

func (c Columns) HasPrimaryKey() bool {
	return supporting.ContainsWithMatcher(c, func(other *Column) bool {
		return other.IsPrimaryKey()
	})
}

func (c Columns) PrimaryKeyIndex() (index *Index, present bool) {
	if !c.HasPrimaryKey() {
		return nil, false
	}

	primaryKeyColumns := supporting.Filter(c, func(item *Column) bool {
		return item.IsPrimaryKey()
	})

	supporting.Sort(primaryKeyColumns, func(this, other *Column) bool {
		return *this.primaryKeySeq < *other.primaryKeySeq
	})

	firstColumn := primaryKeyColumns[0]
	return newIndex(
		*firstColumn.indexName, primaryKeyColumns, firstColumn.primaryKey, firstColumn.isReplicaIdent,
	), true
}

type Column struct {
	name           string
	dataType       uint32
	typeName       string
	nullable       bool
	primaryKey     bool
	primaryKeySeq  *int
	defaultValue   *string
	isReplicaIdent bool
	indexName      *string
}

func NewColumn(name string, dataType uint32, typeName string, nullable bool, defaultValue *string) Column {
	return NewIndexColumn(name, dataType, typeName, nullable, false, nil, defaultValue, false, nil)
}

func NewIndexColumn(name string, dataType uint32, typeName string, nullable, primaryKey bool,
	primaryKeySeq *int, defaultValue *string, isReplicaIdent bool, indexName *string) Column {

	return Column{
		name:           name,
		dataType:       dataType,
		typeName:       typeName,
		nullable:       nullable,
		primaryKey:     primaryKey,
		primaryKeySeq:  primaryKeySeq,
		defaultValue:   defaultValue,
		isReplicaIdent: isReplicaIdent,
		indexName:      indexName,
	}
}

func (c Column) Name() string {
	return c.name
}

func (c Column) DataType() uint32 {
	return c.dataType
}

func (c Column) TypeName() string {
	return c.typeName
}

func (c Column) IsNullable() bool {
	return c.nullable
}

func (c Column) IsPrimaryKey() bool {
	return c.primaryKey
}

func (c Column) DefaultValue() *string {
	return c.defaultValue
}

func (c Column) equals(other Column) bool {
	return c.name == other.name &&
		c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue))
}

func (c Column) equalsExceptName(other Column) bool {
	return c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue))
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
