package schemamodel

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"strconv"
)

type SchemaBuilder interface {
	SchemaType() Type
	FieldName(fieldName string) SchemaBuilder
	GetFieldName() string
	SchemaName(schemaName string) SchemaBuilder
	GetSchemaName() string
	Optional() SchemaBuilder
	Required() SchemaBuilder
	SetOptional(optional bool) SchemaBuilder
	IsOptional() bool
	DefaultValue(defaultValue *string) SchemaBuilder
	GetDefaultValue() *string
	Version(version int) SchemaBuilder
	GetVersion() int
	Index(index int) SchemaBuilder
	GetIndex() int
	Documentation(documentation *string) SchemaBuilder
	GetDocumentation() *string
	Parameter(key string, value any) SchemaBuilder
	Parameters() map[string]any
	Field(name FieldName, index int, schemaBuilder SchemaBuilder) SchemaBuilder
	Fields() map[string]Field
	KeySchema(schema Struct) SchemaBuilder
	ValueSchema(schema Struct) SchemaBuilder
	Clone() SchemaBuilder
	Build() Struct
}

type Field interface {
	Index() int
	SchemaStruct() Struct
	SchemaBuilder() SchemaBuilder
}

type fieldImpl struct {
	name          string
	index         int
	schemaStruct  Struct
	schemaBuilder SchemaBuilder
}

func (f *fieldImpl) Index() int {
	return f.index
}

func (f *fieldImpl) SchemaStruct() Struct {
	return f.schemaStruct
}

func (f *fieldImpl) SchemaBuilder() SchemaBuilder {
	return f.schemaBuilder
}

type schemaBuilderImpl struct {
	fieldName     string
	schemaName    string
	schemaType    Type
	version       int
	optional      bool
	defaultValue  *string
	documentation *string
	index         int
	parameters    map[string]any
	fields        map[string]Field
	keySchema     Struct
	valueSchema   Struct
}

func NewSchemaBuilder(schemaType Type) SchemaBuilder {
	return &schemaBuilderImpl{
		schemaType: schemaType,
		index:      -1,
	}
}

func (s *schemaBuilderImpl) SchemaType() Type {
	return s.schemaType
}

func (s *schemaBuilderImpl) FieldName(fieldName string) SchemaBuilder {
	s.fieldName = fieldName
	return s
}

func (s *schemaBuilderImpl) GetFieldName() string {
	return s.fieldName
}

func (s *schemaBuilderImpl) SchemaName(schemaName string) SchemaBuilder {
	s.schemaName = schemaName
	return s
}

func (s *schemaBuilderImpl) GetSchemaName() string {
	return s.schemaName
}

func (s *schemaBuilderImpl) Optional() SchemaBuilder {
	s.optional = true
	return s
}

func (s *schemaBuilderImpl) SetOptional(optional bool) SchemaBuilder {
	s.optional = optional
	return s
}

func (s *schemaBuilderImpl) Required() SchemaBuilder {
	s.optional = false
	return s
}

func (s *schemaBuilderImpl) IsOptional() bool {
	return s.optional
}

func (s *schemaBuilderImpl) DefaultValue(defaultValue *string) SchemaBuilder {
	s.defaultValue = defaultValue
	return s
}

func (s *schemaBuilderImpl) GetDefaultValue() *string {
	return s.defaultValue
}

func (s *schemaBuilderImpl) Version(version int) SchemaBuilder {
	s.version = version
	return s
}

func (s *schemaBuilderImpl) GetVersion() int {
	return s.version
}

func (s *schemaBuilderImpl) Index(index int) SchemaBuilder {
	s.index = index
	return s
}

func (s *schemaBuilderImpl) GetIndex() int {
	return s.index
}

func (s *schemaBuilderImpl) Documentation(documentation *string) SchemaBuilder {
	s.documentation = documentation
	return s
}

func (s *schemaBuilderImpl) GetDocumentation() *string {
	return s.documentation
}

func (s *schemaBuilderImpl) Parameter(key string, value any) SchemaBuilder {
	if s.parameters == nil {
		s.parameters = make(map[string]any)
	}
	s.parameters[key] = value
	return s
}

func (s *schemaBuilderImpl) Parameters() map[string]any {
	return s.parameters
}

func (s *schemaBuilderImpl) Field(name FieldName, index int, schemaBuilder SchemaBuilder) SchemaBuilder {
	if s.fields == nil {
		s.fields = make(map[string]Field)
	}
	s.fields[name] = &fieldImpl{
		name:          name,
		index:         index,
		schemaBuilder: schemaBuilder.Clone().FieldName(name),
	}
	return s
}

func (s *schemaBuilderImpl) Fields() map[string]Field {
	return s.fields
}

func (s *schemaBuilderImpl) KeySchema(schema Struct) SchemaBuilder {
	s.keySchema = schema
	return s
}

func (s *schemaBuilderImpl) ValueSchema(schema Struct) SchemaBuilder {
	s.valueSchema = schema
	return s
}

func (s *schemaBuilderImpl) Clone() SchemaBuilder {
	return &schemaBuilderImpl{
		fieldName:     s.fieldName,
		schemaName:    s.schemaName,
		schemaType:    s.schemaType,
		version:       s.version,
		optional:      s.optional,
		defaultValue:  s.defaultValue,
		documentation: s.documentation,
		parameters:    s.parameters,
		fields:        s.fields,
		keySchema:     s.keySchema,
		valueSchema:   s.valueSchema,
	}
}

func (s *schemaBuilderImpl) Build() Struct {
	schemaStruct := Struct{
		FieldNameType: s.schemaType,
	}
	switch s.schemaType {
	case ARRAY:
		schemaStruct[FieldNameValueSchema] = s.valueSchema
	case MAP:
		schemaStruct[FieldNameKeySchema] = s.keySchema
		schemaStruct[FieldNameValueSchema] = s.valueSchema
	case STRUCT:
		fields := supporting.MapMapper(s.fields, func(key string, element Field) Field {
			return element
		})

		supporting.Sort(fields, func(this, other Field) bool {
			return this.Index() < other.Index()
		})

		fieldSchemas := make([]Struct, 0)
		for _, field := range fields {
			if field.SchemaStruct() != nil {
				fieldSchemas = append(fieldSchemas, field.SchemaStruct())
			} else {
				fieldSchemas = append(fieldSchemas, field.SchemaBuilder().Build())
			}
		}

		schemaStruct[FieldNameFields] = fieldSchemas
	}

	if s.fieldName != "" {
		schemaStruct[FieldNameField] = s.fieldName
	}

	if s.schemaName != "" {
		schemaStruct[FieldNameName] = s.schemaName
	}

	if s.index > -1 {
		schemaStruct[FieldNameIndex] = s.index
	}
	if s.optional {
		schemaStruct[FieldNameOptional] = true
	}

	if s.defaultValue != nil {
		defaultValue := *s.defaultValue
		if v, err := strconv.ParseBool(defaultValue); err == nil {
			schemaStruct[FieldNameDefault] = v
		} else if v, err := strconv.ParseInt(defaultValue, 10, 64); err == nil {
			schemaStruct[FieldNameDefault] = v
		} else if v, err := strconv.ParseFloat(defaultValue, 64); err == nil {
			schemaStruct[FieldNameDefault] = v
		} else {
			schemaStruct[FieldNameDefault] = defaultValue
		}
	}

	for key, value := range s.parameters {
		schemaStruct[key] = value
	}

	return schemaStruct
}
