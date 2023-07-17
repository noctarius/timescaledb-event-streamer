package datatypes

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type arraySchemaBuilder struct {
	pgType systemcatalog.PgType
}

func (a *arraySchemaBuilder) SchemaType() schemamodel.Type {
	return a.pgType.SchemaType()
}

func (a *arraySchemaBuilder) Schema(column schemamodel.ColumnDescriptor) schemamodel.Struct {
	elementType := a.pgType.ElementType()

	elementColumnName := fmt.Sprintf("%s-element", column.Name())
	elementSchema := schemamodel.Struct{
		schemamodel.FieldNameType:     elementType.SchemaType(),
		schemamodel.FieldNameOptional: true,
		schemamodel.FieldNameField:    elementColumnName,
	}

	return schemamodel.Struct{
		schemamodel.FieldNameType:     schemamodel.ARRAY,
		schemamodel.FieldNameOptional: column.IsNullable(),
		schemamodel.FieldNameField:    column.Name(),
		"valueSchema":                 elementSchema,
	}
}
