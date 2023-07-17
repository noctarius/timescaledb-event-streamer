package systemcatalog

import "github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"

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
	SchemaType() schemamodel.Type
	Schema() schemamodel.Schema
	Equal(other PgType) bool
}
