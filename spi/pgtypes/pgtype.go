package pgtypes

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
)

// Converter represents a conversion function to convert from
// a PostgreSQL internal OID number and value to a value according
// to the stream definition
type Converter func(oid uint32, value any) (any, error)

type PgCategory string

const (
	Array       PgCategory = "A"
	Boolean     PgCategory = "B"
	Composite   PgCategory = "C"
	DateTime    PgCategory = "D"
	Enum        PgCategory = "E"
	Geometric   PgCategory = "G"
	Network     PgCategory = "I"
	Numeric     PgCategory = "N"
	Pseudo      PgCategory = "P"
	Range       PgCategory = "R"
	String      PgCategory = "S"
	Timespan    PgCategory = "T"
	UserDefined PgCategory = "D"
	BitString   PgCategory = "V"
	Unknown     PgCategory = "X"
	InternalUse PgCategory = "Z"
)

type PgKind string

const (
	BaseKind       PgKind = "b"
	CompositeKind  PgKind = "c"
	DomainKind     PgKind = "d"
	EnumKind       PgKind = "e"
	PseudoKind     PgKind = "p"
	RangeKind      PgKind = "r"
	MultiRangeKind PgKind = "m"
)

type PgType interface {
	Namespace() string
	Name() string
	Kind() PgKind
	Oid() uint32
	Category() PgCategory
	IsArray() bool
	IsRecord() bool
	ArrayType() PgType
	ElementType() PgType
	ParentType() PgType
	OidArray() uint32
	OidElement() uint32
	OidParent() uint32
	Modifiers() int
	EnumValues() []string
	Delimiter() string
	SchemaType() schema.Type
	SchemaBuilder() schema.Builder
	Format() string
	Equal(
		other PgType,
	) bool
}

type pgType struct {
	namespace  string
	name       string
	kind       PgKind
	oid        uint32
	category   PgCategory
	arrayType  bool
	recordType bool
	oidArray   uint32
	oidElement uint32
	oidParent  uint32
	modifiers  int
	enumValues []string
	delimiter  string
	schemaType schema.Type

	typeManager         *typeManager
	schemaBuilder       schema.Builder
	resolvedArrayType   PgType
	resolvedElementType PgType
	resolvedParentType  PgType
}

func newType(
	typeManager *typeManager, namespace, name string, kind PgKind, oid uint32,
	category PgCategory, arrayType, recordType bool, oidArray, oidElement, oidParent uint32,
	modifiers int, enumValues []string, delimiter string,
) PgType {

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

func (t *pgType) Kind() PgKind {
	return t.kind
}

func (t *pgType) Oid() uint32 {
	return t.oid
}

func (t *pgType) Category() PgCategory {
	return t.category
}

func (t *pgType) IsArray() bool {
	return t.arrayType
}

func (t *pgType) IsRecord() bool {
	return t.recordType
}

func (t *pgType) ArrayType() PgType {
	if t.resolvedArrayType == nil {
		arrayType, err := t.typeManager.DataType(t.oidArray)
		if err != nil {
			panic(err)
		}
		t.resolvedArrayType = arrayType
	}
	return t.resolvedArrayType
}

func (t *pgType) ElementType() PgType {
	if t.resolvedElementType == nil {
		elementType, err := t.typeManager.DataType(t.oidElement)
		if err != nil {
			panic(err)
		}
		t.resolvedElementType = elementType
	}
	return t.resolvedElementType
}

func (t *pgType) ParentType() PgType {
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

func (t *pgType) SchemaType() schema.Type {
	return t.schemaType
}

func (t *pgType) SchemaBuilder() schema.Builder {
	if t.schemaBuilder == nil {
		t.schemaBuilder = t.typeManager.resolveSchemaBuilder(t)
	}
	return t.schemaBuilder.Clone()
}

func (t *pgType) Format() string {
	if t.IsArray() {
		return fmt.Sprintf("%s[]", t.ElementType().Format())
	}
	return t.Name()
}

func (t *pgType) Equal(
	other PgType,
) bool {

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
