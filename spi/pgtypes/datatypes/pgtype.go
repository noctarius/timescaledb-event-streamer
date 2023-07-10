package datatypes

// SchemaType is a string like definition of the available
// event stream data types
type SchemaType string

const (
	INT8    SchemaType = "int8"
	INT16   SchemaType = "int16"
	INT32   SchemaType = "int32"
	INT64   SchemaType = "int64"
	FLOAT32 SchemaType = "float32"
	FLOAT64 SchemaType = "float64"
	BOOLEAN SchemaType = "boolean"
	STRING  SchemaType = "string"
	BYTES   SchemaType = "bytes"
	ARRAY   SchemaType = "array"
	MAP     SchemaType = "map"
	STRUCT  SchemaType = "struct"
)

type TypeCategory string

const (
	Array       TypeCategory = "A"
	Boolean     TypeCategory = "B"
	Composite   TypeCategory = "C"
	DateTime    TypeCategory = "D"
	Enum        TypeCategory = "E"
	Geometric   TypeCategory = "G"
	Network     TypeCategory = "I"
	Numeric     TypeCategory = "N"
	Pseudo      TypeCategory = "P"
	Range       TypeCategory = "R"
	String      TypeCategory = "S"
	Timespan    TypeCategory = "T"
	UserDefined TypeCategory = "D"
	BitString   TypeCategory = "V"
	Unknown     TypeCategory = "X"
	InternalUse TypeCategory = "Z"
)

type TypeType string

const (
	BaseType       TypeType = "b"
	CompositeType  TypeType = "c"
	DomainType     TypeType = "d"
	EnumType       TypeType = "e"
	PseudoType     TypeType = "p"
	RangeType      TypeType = "r"
	MultiRangeType TypeType = "m"
)

// Converter represents a conversion function to convert from
// a PostgreSQL internal OID number and value to a value according
// to the stream definition
type Converter func(oid uint32, value any) (any, error)

type Type struct {
	name       string
	typ        TypeType
	oid        uint32
	category   TypeCategory
	arrayType  bool
	arrayOid   uint32
	elementOid uint32
	recordType bool
	parentOid  uint32
	modifiers  int
	enumValues []string
	delimiter  string
	schemaType SchemaType

	typeManager *TypeManager
}

func NewType(name string, typ TypeType, oid uint32, category TypeCategory, arrayType bool, arrayOid uint32,
	elementOid uint32, recordType bool, parentOid uint32, modifiers int, enumValues []string, delimiter string,
	schemaType SchemaType) Type {

	return Type{
		name:       name,
		typ:        typ,
		oid:        oid,
		category:   category,
		arrayType:  arrayType,
		arrayOid:   arrayOid,
		elementOid: elementOid,
		recordType: recordType,
		parentOid:  parentOid,
		modifiers:  modifiers,
		enumValues: enumValues,
		delimiter:  delimiter,
		schemaType: schemaType,
	}
}

func (t Type) Name() string {
	return t.name
}

func (t Type) Typ() TypeType {
	return t.typ
}

func (t Type) Oid() uint32 {
	return t.oid
}

func (t Type) Category() TypeCategory {
	return t.category
}

func (t Type) IsArray() bool {
	return t.arrayType
}

func (t Type) ArrayType() Type {
	arrayType, err := t.typeManager.DataType(t.arrayOid)
	if err != nil {
		panic(err)
	}
	return arrayType
}

func (t Type) ElementType() Type {
	elementType, err := t.typeManager.DataType(t.elementOid)
	if err != nil {
		panic(err)
	}
	return elementType
}

func (t Type) ParentType() Type {
	parentType, err := t.typeManager.DataType(t.parentOid)
	if err != nil {
		panic(err)
	}
	return parentType
}

func (t Type) OidArray() uint32 {
	return t.arrayOid
}

func (t Type) OidElement() uint32 {
	return t.elementOid
}

func (t Type) IsRecord() bool {
	return t.recordType
}

func (t Type) ParentOid() uint32 {
	return t.parentOid
}

func (t Type) Modifiers() int {
	return t.modifiers
}

func (t Type) EnumValues() []string {
	if t.enumValues == nil {
		return []string{}
	}
	return t.enumValues
}

func (t Type) SchemaType() SchemaType {
	return t.schemaType
}

func (t Type) Equal(other Type) bool {
	return t.name == other.name &&
		t.typ == other.typ &&
		t.oid == other.oid &&
		t.category == other.category &&
		t.arrayType == other.arrayType &&
		t.arrayOid == other.arrayOid &&
		t.elementOid == other.elementOid &&
		t.recordType == other.recordType &&
		t.parentOid == other.parentOid &&
		t.modifiers == other.modifiers &&
		t.delimiter == other.delimiter &&
		t.schemaType == other.schemaType &&
		stringArrayEqual(t.enumValues, other.enumValues)
}

// GetSchemaType returns the DataType for a given OID.
func GetSchemaType(oid uint32, arrayType bool, typType TypeType) SchemaType {
	if coreType, present := coreTypes[oid]; present {
		return coreType
	}
	if arrayType {
		return ARRAY
	} else if typType == EnumType {
		return STRING
	}
	return STRUCT
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
