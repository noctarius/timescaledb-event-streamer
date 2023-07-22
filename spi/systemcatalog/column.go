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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"strings"
)

// Columns represents a collection of columns which
// may or may not represent an index
type Columns []Column

// SnapshotIndex returns the index used for snapshot watermarks. This
// can either be an actual index, such as a primary key, or a virtual
// index based on the dimensions of the hypertable.
func (c Columns) SnapshotIndex() (index *Index, present bool) {
	if index, present := c.PrimaryKeyIndex(); present {
		return index, true
	}

	dimensionColumns := supporting.Filter(c, func(item Column) bool {
		return item.IsDimension()
	})

	supporting.Sort(dimensionColumns, func(this, other Column) bool {
		return *this.dimSeq < *other.dimSeq
	})

	return newIndex(
		"dimensions", dimensionColumns, false, false,
	), true
}

// HasPrimaryKey returns true if the collection of columns contains
// one or more primary key column(s)
func (c Columns) HasPrimaryKey() bool {
	return supporting.ContainsWithMatcher(c, func(other Column) bool {
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

	primaryKeyColumns := supporting.Filter(c, func(item Column) bool {
		return item.IsPrimaryKey()
	})

	supporting.Sort(primaryKeyColumns, func(this, other Column) bool {
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
	return supporting.ContainsWithMatcher(c, func(other Column) bool {
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

	replicaIdentityColumns := supporting.Filter(c, func(item Column) bool {
		return item.IsReplicaIdent()
	})

	supporting.Sort(replicaIdentityColumns, func(this, other Column) bool {
		return *this.keySeq < *other.keySeq
	})

	firstColumn := replicaIdentityColumns[0]
	return newIndex(
		*firstColumn.indexName, replicaIdentityColumns, false, true,
	), true
}

// Column represents a column from a hypertable or index
type Column struct {
	name          string
	dataType      uint32
	modifiers     int
	pgType        pgtypes.PgType
	nullable      bool
	primaryKey    bool
	keySeq        *int
	defaultValue  *string
	replicaIdent  bool
	indexName     *string
	sortOrder     IndexSortOrder
	nullsOrder    IndexNullsOrder
	dimension     bool
	dimAligned    bool
	dimType       *string
	dimSeq        *int
	maxCharLength *int
}

// NewColumn instantiates a new Column instance which isn't
// part of any index. This method is a shorthand version of
// NewIndexColumn
func NewColumn(name string, dataType uint32, modifiers int,
	pgType pgtypes.PgType, nullable bool, defaultValue *string) Column {

	return NewIndexColumn(
		name, dataType, modifiers, pgType, nullable, false, nil,
		defaultValue, false, nil, ASC, NULLS_LAST,
		false, false, nil, nil, nil,
	)
}

// NewIndexColumn instantiates a new Column instance
func NewIndexColumn(name string, dataType uint32, modifiers int, pgType pgtypes.PgType,
	nullable, primaryKey bool, keySeq *int, defaultValue *string, isReplicaIdent bool,
	indexName *string, sortOrder IndexSortOrder, nullsOrder IndexNullsOrder,
	dimension, dimAligned bool, dimType *string, dimSeq, maxCharLength *int) Column {

	return Column{
		name:          name,
		dataType:      dataType,
		modifiers:     modifiers,
		pgType:        pgType,
		nullable:      nullable,
		primaryKey:    primaryKey,
		keySeq:        keySeq,
		defaultValue:  defaultValue,
		replicaIdent:  isReplicaIdent,
		indexName:     indexName,
		sortOrder:     sortOrder,
		nullsOrder:    nullsOrder,
		dimension:     dimension,
		dimAligned:    dimAligned,
		dimType:       dimType,
		dimSeq:        dimSeq,
		maxCharLength: maxCharLength,
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

// Modifiers returns the type specific modifiers for this
// column. If no specific modifiers is set, -1 is returned
func (c Column) Modifiers() int {
	return c.modifiers
}

// PgType returns the PG type of the column
func (c Column) PgType() pgtypes.PgType {
	return c.pgType
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

// IsDimension returns true if the column is used
// as a dimension of the hypertable
func (c Column) IsDimension() bool {
	return c.dimension
}

// IsDimensionAligned returns true if the range of
// the dimension is aligned
func (c Column) IsDimensionAligned() bool {
	return c.dimAligned
}

// DimensionType returns the type (`space` or `time`)
// of the dimension. If not a dimension, this function
// returns nil.
func (c Column) DimensionType() *string {
	return c.dimType
}

// MaxCharLength returns the maximum number of
// characters necessary to represent the value
// as a string (if the type is type limited),
// otherwise it returns nil
func (c Column) MaxCharLength() *int {
	return c.maxCharLength
}

func (c Column) SchemaType() schema.Type {
	return c.pgType.SchemaType()
}

// SchemaBuilder returns a schema builder based on the
// column's PgType and internal state (default value,
// nullable, name, etc).
func (c Column) SchemaBuilder() schema.SchemaBuilder {
	schemaBuilder := c.pgType.SchemaBuilder().
		FieldName(c.Name()).
		DefaultValue(c.defaultValue).
		SetOptional(c.IsNullable())

	valueLength := c.valueLength()
	if valueLength > -1 {
		schemaBuilder.Parameter(schema.FieldNameLength, valueLength)
	}

	return schemaBuilder.Clone()
}

func (c Column) Format() string {
	builder := strings.Builder{}
	builder.WriteString(c.name)
	builder.WriteString(":")
	if c.pgType.IsArray() {
		builder.WriteString(c.pgType.ElementType().Name())
	} else {
		builder.WriteString(c.pgType.Name())
	}
	valueLength := c.valueLength()
	if valueLength > -1 {
		builder.WriteString(fmt.Sprintf("(%d)", valueLength))
	}
	if c.pgType.IsArray() {
		builder.WriteString("[]")
	}
	return builder.String()
}

func (c Column) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("name:%s ", c.name))
	builder.WriteString(fmt.Sprintf("dataType:%d ", c.dataType))
	builder.WriteString(fmt.Sprintf("modifiers:%d ", c.modifiers))
	builder.WriteString(fmt.Sprintf("pgType:%s ", c.pgType.Name()))
	builder.WriteString(fmt.Sprintf("nullable:%t ", c.nullable))
	builder.WriteString(fmt.Sprintf("primaryKey:%t ", c.primaryKey))
	if c.keySeq == nil {
		builder.WriteString("keySeq:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("keySeq:%d ", *c.keySeq))
	}
	if c.indexName == nil {
		builder.WriteString("indexName:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("indexName:%s ", *c.indexName))
	}
	builder.WriteString(fmt.Sprintf("replicaIdent:%t ", c.replicaIdent))
	if c.defaultValue == nil {
		builder.WriteString("defaultValue:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("defaultValue:%s ", *c.defaultValue))
	}
	builder.WriteString(fmt.Sprintf("dimension:%t ", c.dimension))
	builder.WriteString(fmt.Sprintf("dimAligned:%t ", c.dimAligned))
	if c.dimType == nil {
		builder.WriteString("dimType:<nil> ")
	} else {
		builder.WriteString(fmt.Sprintf("dimType:%s ", *c.dimType))
	}
	if c.dimSeq == nil {
		builder.WriteString("dimSeq:<nil>")
	} else {
		builder.WriteString(fmt.Sprintf("dimSeq:%d", *c.dimSeq))
	}
	if c.maxCharLength == nil {
		builder.WriteString("maxCharLength:<nil>")
	} else {
		builder.WriteString(fmt.Sprintf("maxCharLength:%d", *c.maxCharLength))
	}
	builder.WriteString("}")
	return builder.String()
}

func (c Column) equals(other Column) bool {
	return c.name == other.name &&
		c.pgType.Equal(other.pgType) &&
		c.dataType == other.dataType &&
		c.modifiers == other.modifiers &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		c.replicaIdent == other.replicaIdent &&
		((c.keySeq == nil && other.keySeq == nil) ||
			(c.keySeq != nil && other.keySeq != nil && *c.keySeq == *other.keySeq)) &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue)) &&
		((c.indexName == nil && other.indexName == nil) ||
			(c.indexName != nil && other.indexName != nil && *c.indexName == *other.indexName)) &&
		c.dimension == other.dimension &&
		c.dimAligned == other.dimAligned &&
		((c.dimType == nil && other.dimType == nil) ||
			(c.dimType != nil && other.dimType != nil && *c.dimType == *other.dimType)) &&
		((c.dimSeq == nil && other.dimSeq == nil) ||
			(c.dimSeq != nil && other.dimSeq != nil && *c.dimSeq == *other.dimSeq)) &&
		((c.maxCharLength == nil && other.maxCharLength == nil) ||
			(c.maxCharLength != nil && other.maxCharLength != nil && *c.maxCharLength == *other.maxCharLength))
}

func (c Column) equalsExceptName(other Column) bool {
	return c.pgType.Equal(other.pgType) &&
		c.dataType == other.dataType &&
		c.modifiers == other.modifiers &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		c.replicaIdent == other.replicaIdent &&
		((c.keySeq == nil && other.keySeq == nil) ||
			(c.keySeq != nil && other.keySeq != nil && *c.keySeq == *other.keySeq)) &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue)) &&
		((c.indexName == nil && other.indexName == nil) ||
			(c.indexName != nil && other.indexName != nil && *c.indexName == *other.indexName)) &&
		c.dimension == other.dimension &&
		c.dimAligned == other.dimAligned &&
		((c.dimType == nil && other.dimType == nil) ||
			(c.dimType != nil && other.dimType != nil && *c.dimType == *other.dimType)) &&
		((c.dimSeq == nil && other.dimSeq == nil) ||
			(c.dimSeq != nil && other.dimSeq != nil && *c.dimSeq == *other.dimSeq)) &&
		((c.maxCharLength == nil && other.maxCharLength == nil) ||
			(c.maxCharLength != nil && other.maxCharLength != nil && *c.maxCharLength == *other.maxCharLength))
}

func (c Column) differences(new Column) map[string]string {
	differences := make(map[string]string, 0)
	if c.name != new.name {
		differences["name"] = fmt.Sprintf("%s=>%s", c.name, new.name)
	}
	if c.dataType != new.dataType {
		differences["dataType"] = fmt.Sprintf("%d=>%d", c.dataType, new.dataType)
	}
	if c.modifiers != new.modifiers {
		differences["modifiers"] = fmt.Sprintf("%d=>%d", c.modifiers, new.modifiers)
	}
	if !c.pgType.Equal(new.pgType) {
		differences["pgType"] = fmt.Sprintf("%s=>%s", c.pgType.Name(), new.pgType.Name())
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
	if c.dimension != new.dimension {
		differences["dimension"] = fmt.Sprintf("%t=>%t", c.dimension, new.dimension)
	}
	if c.dimAligned != new.dimAligned {
		differences["dimAligned"] = fmt.Sprintf("%t=>%t", c.dimAligned, new.dimAligned)
	}
	if (c.dimType == nil && new.dimType != nil) ||
		(c.dimType != nil && new.dimType == nil) {
		o := "<nil>"
		if c.dimType != nil {
			o = *c.dimType
		}
		n := "<nil>"
		if new.dimType != nil {
			n = *new.dimType
		}
		differences["dimType"] = fmt.Sprintf("%s=>%s", o, n)
	}
	if (c.dimSeq == nil && new.dimSeq != nil) ||
		(c.dimSeq != nil && new.dimSeq == nil) {
		o := "<nil>"
		if c.dimSeq != nil {
			o = fmt.Sprintf("%d", *c.dimSeq)
		}
		n := "<nil>"
		if new.dimSeq != nil {
			o = fmt.Sprintf("%d", *new.dimSeq)
		}
		differences["dimSeq"] = fmt.Sprintf("%s=>%s", o, n)
	}
	if (c.maxCharLength == nil && new.maxCharLength != nil) ||
		(c.maxCharLength != nil && new.maxCharLength == nil) {
		o := "<nil>"
		if c.maxCharLength != nil {
			o = fmt.Sprintf("%d", *c.maxCharLength)
		}
		n := "<nil>"
		if new.maxCharLength != nil {
			o = fmt.Sprintf("%d", *new.maxCharLength)
		}
		differences["maxCharLength"] = fmt.Sprintf("%s=>%s", o, n)
	}
	return differences
}

func (c Column) valueLength() int {
	switch c.dataType {
	case pgtype.BitOID, pgtype.VarbitOID, pgtype.BPCharOID, pgtype.VarcharOID:
		if c.maxCharLength != nil {
			return *c.maxCharLength
		}

	case pgtype.BitArrayOID, pgtype.VarbitArrayOID:
		if c.modifiers > 0 {
			return c.pgType.Modifiers()
		}

	case pgtype.BPCharArrayOID, pgtype.VarcharArrayOID:
		if c.modifiers > 4 { // FIXME: 4 is only assumed to be true on systems (size_of(int32))
			return c.Modifiers() - 4
		}
	}
	return -1
}
