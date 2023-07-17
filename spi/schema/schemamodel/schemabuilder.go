package schemamodel

type Schema interface {
	SchemaType() Type
	Schema(column ColumnDescriptor) Struct
}

type ColumnDescriptor interface {
	Name() string
	DataType() uint32
	IsNullable() bool
	IsPrimaryKey() bool
	IsReplicaIdent() bool
	DefaultValue() *string
	IsDimension() bool
	IsDimensionAligned() bool
	DimensionType() *string
	String() string
}

var (
	schemaInt8    = &simpleTypeSchema{schemaType: INT8}
	schemaInt16   = &simpleTypeSchema{schemaType: INT16}
	schemaInt32   = &simpleTypeSchema{schemaType: INT32}
	schemaInt64   = &simpleTypeSchema{schemaType: INT64}
	schemaFloat32 = &simpleTypeSchema{schemaType: FLOAT32}
	schemaFloat64 = &simpleTypeSchema{schemaType: FLOAT64}
	schemaBoolean = &simpleTypeSchema{schemaType: BOOLEAN}
	schemaString  = &simpleTypeSchema{schemaType: STRING}
)

func Int8() Schema {
	return schemaInt8
}

func Int16() Schema {
	return schemaInt16
}

func Int32() Schema {
	return schemaInt32
}

func Int64() Schema {
	return schemaInt64
}

func Float32() Schema {
	return schemaFloat32
}

func Float64() Schema {
	return schemaFloat64
}

func Boolean() Schema {
	return schemaBoolean
}

func String() Schema {
	return schemaString
}

type simpleTypeSchema struct {
	schemaType Type
}

func (s *simpleTypeSchema) SchemaType() Type {
	return s.schemaType
}

func (s *simpleTypeSchema) Schema(_ ColumnDescriptor) Struct {
	return nil
}
