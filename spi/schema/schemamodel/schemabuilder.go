package schemamodel

type SchemaBuilder interface {
	SchemaType() SchemaType
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
	schemaBuilderInt8    = &simpleTypeSchemaBuilder{schemaType: INT8}
	schemaBuilderInt16   = &simpleTypeSchemaBuilder{schemaType: INT16}
	schemaBuilderInt32   = &simpleTypeSchemaBuilder{schemaType: INT32}
	schemaBuilderInt64   = &simpleTypeSchemaBuilder{schemaType: INT64}
	schemaBuilderFloat32 = &simpleTypeSchemaBuilder{schemaType: FLOAT32}
	schemaBuilderFloat64 = &simpleTypeSchemaBuilder{schemaType: FLOAT64}
	schemaBuilderBoolean = &simpleTypeSchemaBuilder{schemaType: BOOLEAN}
	schemaBuilderString  = &simpleTypeSchemaBuilder{schemaType: STRING}
)

func Int8() SchemaBuilder {
	return schemaBuilderInt8
}

func Int16() SchemaBuilder {
	return schemaBuilderInt16
}

func Int32() SchemaBuilder {
	return schemaBuilderInt32
}

func Int64() SchemaBuilder {
	return schemaBuilderInt64
}

func Float32() SchemaBuilder {
	return schemaBuilderFloat32
}

func Float64() SchemaBuilder {
	return schemaBuilderFloat64
}

func Boolean() SchemaBuilder {
	return schemaBuilderBoolean
}

func String() SchemaBuilder {
	return schemaBuilderString
}

type simpleTypeSchemaBuilder struct {
	schemaType SchemaType
}

func (s *simpleTypeSchemaBuilder) SchemaType() SchemaType {
	return s.schemaType
}

func (s *simpleTypeSchemaBuilder) Schema(_ ColumnDescriptor) Struct {
	return nil
}
