package pgtypes

import "github.com/noctarius/timescaledb-event-streamer/spi/schema"

// TypeConverter represents a conversion function to convert from
// a PostgreSQL internal OID number and value to a value according
// to the stream definition
type TypeConverter func(oid uint32, value any) (any, error)

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

type TypeFactory func(namespace, name string, kind PgKind, oid uint32, category PgCategory,
	arrayType bool, recordType bool, oidArray uint32, oidElement uint32, oidParent uint32,
	modifiers int, enumValues []string, delimiter string) PgType
