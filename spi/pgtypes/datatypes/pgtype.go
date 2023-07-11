package datatypes

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

// Converter represents a conversion function to convert from
// a PostgreSQL internal OID number and value to a value according
// to the stream definition
type Converter func(oid uint32, value any) (any, error)

type pgType struct {
	name       string
	kind       systemcatalog.PgKind
	oid        uint32
	category   systemcatalog.PgCategory
	arrayType  bool
	recordType bool
	oidArray   uint32
	oidElement uint32
	oidParent  uint32
	modifiers  int
	enumValues []string
	delimiter  string
	schemaType schemamodel.SchemaType

	typeManager         *TypeManager
	schemaBuilder       schemamodel.SchemaBuilder
	resolvedArrayType   systemcatalog.PgType
	resolvedElementType systemcatalog.PgType
	resolvedParentType  systemcatalog.PgType
}

func newType(name string, kind systemcatalog.PgKind, oid uint32, category systemcatalog.PgCategory,
	arrayType bool, recordType bool, oidArray uint32, oidElement uint32, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) *pgType {

	return &pgType{
		name:       name,
		kind:       kind,
		oid:        oid,
		category:   category,
		arrayType:  arrayType,
		recordType: recordType,
		oidArray:   oidArray,
		oidElement: oidElement,
		oidParent:  oidParent,
		modifiers:  modifiers,
		enumValues: enumValues,
		delimiter:  delimiter,
		schemaType: getSchemaType(oid, arrayType, kind),
	}
}

func (t *pgType) Name() string {
	return t.name
}

func (t *pgType) Kind() systemcatalog.PgKind {
	return t.kind
}

func (t *pgType) Oid() uint32 {
	return t.oid
}

func (t *pgType) Category() systemcatalog.PgCategory {
	return t.category
}

func (t *pgType) IsArray() bool {
	return t.arrayType
}

func (t *pgType) IsRecord() bool {
	return t.recordType
}

func (t *pgType) ArrayType() systemcatalog.PgType {
	if t.resolvedArrayType == nil {
		arrayType, err := t.typeManager.DataType(t.oidArray)
		if err != nil {
			panic(err)
		}
		t.resolvedArrayType = arrayType
	}
	return t.resolvedArrayType
}

func (t *pgType) ElementType() systemcatalog.PgType {
	if t.resolvedElementType == nil {
		elementType, err := t.typeManager.DataType(t.oidElement)
		if err != nil {
			panic(err)
		}
		t.resolvedElementType = elementType
	}
	return t.resolvedElementType
}

func (t *pgType) ParentType() systemcatalog.PgType {
	if t.resolvedParentType == nil {
		parentType, err := t.typeManager.DataType(t.oidParent)
		if err != nil {
			panic(err)
		}
		t.resolvedParentType = parentType
	}
	return t.resolvedParentType
}

func (t *pgType) OidArray() uint32 {
	return t.oidArray
}

func (t *pgType) OidElement() uint32 {
	return t.oidElement
}

func (t *pgType) OidParent() uint32 {
	return t.oidParent
}

func (t *pgType) Modifiers() int {
	return t.modifiers
}

func (t *pgType) EnumValues() []string {
	if t.enumValues == nil {
		return []string{}
	}
	return t.enumValues
}

func (t *pgType) Delimiter() string {
	return t.delimiter
}

func (t *pgType) SchemaType() schemamodel.SchemaType {
	return t.schemaType
}

func (t *pgType) SchemaBuilder() schemamodel.SchemaBuilder {
	return t.schemaBuilder
}

func (t *pgType) Equal(other systemcatalog.PgType) bool {
	return t.name == other.Name() &&
		t.kind == other.Kind() &&
		t.oid == other.Oid() &&
		t.category == other.Category() &&
		t.arrayType == other.IsArray() &&
		t.oidArray == other.OidArray() &&
		t.oidElement == other.OidElement() &&
		t.recordType == other.IsRecord() &&
		t.oidParent == other.OidParent() &&
		t.modifiers == other.Modifiers() &&
		t.delimiter == other.Delimiter() &&
		t.schemaType == other.SchemaType() &&
		stringArrayEqual(t.enumValues, other.EnumValues())
}

func (t *pgType) resolveSchemaBuilder() schemamodel.SchemaBuilder {
	if t.IsArray() {
		return &arraySchemaBuilder{pgType: t}
	}
	return nil //FIXME
}

func stringArrayEqual(this, that []string) bool {
	if (this == nil && that != nil) || (this != nil && that == nil) {
		return false
	}
	if len(this) != len(that) {
		return false
	}
	for i := 0; i < len(this); i++ {
		if this[i] != that[i] {
			return false
		}
	}
	return true
}
