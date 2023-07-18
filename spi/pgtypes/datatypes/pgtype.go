package datatypes

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

// Converter represents a conversion function to convert from
// a PostgreSQL internal OID number and value to a value according
// to the stream definition
type Converter func(oid uint32, value any) (any, error)

type pgType struct {
	namespace  string
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
	schemaType schemamodel.Type

	typeManager         *TypeManager
	schemaBuilder       schemamodel.SchemaBuilder
	resolvedArrayType   systemcatalog.PgType
	resolvedElementType systemcatalog.PgType
	resolvedParentType  systemcatalog.PgType
}

func newType(typeManager *TypeManager, namespace, name string, kind systemcatalog.PgKind, oid uint32,
	category systemcatalog.PgCategory, arrayType, recordType bool, oidArray, oidElement, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) *pgType {

	t := &pgType{
		namespace:   namespace,
		name:        name,
		kind:        kind,
		oid:         oid,
		category:    category,
		arrayType:   arrayType,
		recordType:  recordType,
		oidArray:    oidArray,
		oidElement:  oidElement,
		oidParent:   oidParent,
		modifiers:   modifiers,
		enumValues:  enumValues,
		delimiter:   delimiter,
		typeManager: typeManager,
		schemaType:  typeManager.getSchemaType(oid, arrayType, kind),
	}

	// If this is a primitive type we can resolve early
	if t.schemaType.IsPrimitive() {
		t.schemaBuilder = typeManager.resolveSchemaBuilder(t)
	}

	return t
}

func (t *pgType) Namespace() string {
	return t.namespace
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
	enumValues := make([]string, 0, len(t.enumValues))
	copy(enumValues, t.enumValues)
	return enumValues
}

func (t *pgType) Delimiter() string {
	return t.delimiter
}

func (t *pgType) SchemaType() schemamodel.Type {
	return t.schemaType
}

func (t *pgType) SchemaBuilder() schemamodel.SchemaBuilder {
	if t.schemaBuilder == nil {
		t.schemaBuilder = t.typeManager.resolveSchemaBuilder(t)
	}
	return t.schemaBuilder.Clone()
}

func (t *pgType) Equal(other systemcatalog.PgType) bool {
	return t.namespace == other.Namespace() &&
		t.name == other.Name() &&
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
		supporting.ArrayEqual(t.enumValues, other.EnumValues())
}
