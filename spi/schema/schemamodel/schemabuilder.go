package schemamodel

type SchemaBuilder interface {
	BaseSchemaType() SchemaType
	Schema(oid uint32, modifier int, value any) Struct
}
