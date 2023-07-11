package schemamodel

type SchemaBuilder interface {
	BaseSchemaType() SchemaType
	Schema() Struct
}

var (
	schemaBuilderInt8    = &simpleTypeSchemaBuilder{baseSchemaType: INT8}
	schemaBuilderInt16   = &simpleTypeSchemaBuilder{baseSchemaType: INT16}
	schemaBuilderInt32   = &simpleTypeSchemaBuilder{baseSchemaType: INT32}
	schemaBuilderInt64   = &simpleTypeSchemaBuilder{baseSchemaType: INT64}
	schemaBuilderFloat32 = &simpleTypeSchemaBuilder{baseSchemaType: FLOAT32}
	schemaBuilderFloat64 = &simpleTypeSchemaBuilder{baseSchemaType: FLOAT64}
	schemaBuilderBoolean = &simpleTypeSchemaBuilder{baseSchemaType: BOOLEAN}
	schemaBuilderString  = &simpleTypeSchemaBuilder{baseSchemaType: STRING}
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
	baseSchemaType SchemaType
}

func (s *simpleTypeSchemaBuilder) BaseSchemaType() SchemaType {
	return s.baseSchemaType
}

func (s *simpleTypeSchemaBuilder) Schema() Struct {
	return nil
}
