/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package schema

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/functional"
	"github.com/samber/lo"
	"strconv"
)

const (
	BitsSchemaName      = "io.debezium.data.Bits"
	EnumSchemaName      = "io.debezium.data.Enum"
	LtreeSchemaName     = "io.debezium.data.Lree"
	JsonSchemaName      = "io.debezium.data.Json"
	UuidSchemaName      = "io.debezium.data.Uuid"
	XmlSchemaName       = "io.debezium.data.Xml"
	GeographySchemaName = "io.debezium.data.geometry.Geography"
	GeometrySchemaName  = "io.debezium.data.geometry.Geometry"
	PointSchemaName     = "io.debezium.data.geometry.Point"
)

type Builder interface {
	SchemaType() Type
	FieldName(
		fieldName string,
	) Builder
	GetFieldName() string
	SchemaName(
		schemaName string,
	) Builder
	GetSchemaName() string
	Optional() Builder
	Required() Builder
	SetOptional(
		optional bool,
	) Builder
	IsOptional() bool
	DefaultValue(
		defaultValue *string,
	) Builder
	GetDefaultValue() *string
	Version(
		version int,
	) Builder
	GetVersion() int
	Index(
		index int,
	) Builder
	GetIndex() int
	Documentation(
		documentation *string,
	) Builder
	GetDocumentation() *string
	Parameter(
		key string, value any,
	) Builder
	Parameters() map[string]any
	Field(
		name FieldName, index int, schemaBuilder Builder,
	) Builder
	Fields() map[string]Field
	KeySchema(
		builder Builder,
	) Builder
	GetKeySchema() Builder
	ValueSchema(
		builder Builder,
	) Builder
	GetValueSchema() Builder
	Clone() Builder
	Build() Struct
}

type Field interface {
	Buildable
	Index() int
	SchemaStruct() Struct
}

func Int8() Builder {
	return NewSchemaBuilder(INT8)
}

func Int16() Builder {
	return NewSchemaBuilder(INT16)
}

func Int32() Builder {
	return NewSchemaBuilder(INT32)
}

func Int64() Builder {
	return NewSchemaBuilder(INT64)
}

func Float32() Builder {
	return NewSchemaBuilder(FLOAT32)
}

func Float64() Builder {
	return NewSchemaBuilder(FLOAT64)
}

func Boolean() Builder {
	return NewSchemaBuilder(BOOLEAN)
}

func String() Builder {
	return NewSchemaBuilder(STRING)
}

func Bytes() Builder {
	return NewSchemaBuilder(BYTES)
}

func Enum(
	enumValues []string,
) Builder {

	return String().
		SchemaName(EnumSchemaName).
		Version(1).
		Parameter(FieldNameAllowed, enumValues)
}

func Bits(
	length int,
) Builder {

	return String().
		SchemaName(BitsSchemaName).
		Parameter(FieldNameLength, strconv.FormatInt(int64(length), 10)).
		Version(1)
}

func Ltree() Builder {
	return String().
		SchemaName(LtreeSchemaName).
		Version(1)
}

func Json() Builder {
	return String().
		SchemaName(JsonSchemaName).
		Version(1)
}

func Uuid() Builder {
	return String().
		SchemaName(UuidSchemaName).
		Version(1)
}

func Xml() Builder {
	return String().
		SchemaName(XmlSchemaName).
		Version(1)
}

func Map() Builder {
	return NewSchemaBuilder(MAP)
}

func HStore() Builder {
	return Map().KeySchema(String()).ValueSchema(String().Optional())
}

func Geometry() Builder {
	return NewSchemaBuilder(STRUCT).
		Field("wkb", 0, String()).
		Field("srid", 1, Int32())
}

func Geography() Builder {
	return NewSchemaBuilder(STRUCT).
		Field("wkb", 0, String()).
		Field("srid", 1, Int32())
}

type fieldImpl struct {
	name          string
	index         int
	schemaStruct  Struct
	schemaBuilder Builder
}

func (f *fieldImpl) Index() int {
	return f.index
}

func (f *fieldImpl) SchemaStruct() Struct {
	return f.schemaStruct
}

func (f *fieldImpl) SchemaBuilder() Builder {
	return f.schemaBuilder
}

type schemaBuilderImpl struct {
	fieldName          string
	schemaName         string
	schemaType         Type
	version            int
	optional           bool
	defaultValue       *string
	documentation      *string
	index              int
	parameters         map[string]any
	fields             map[string]Field
	keySchemaBuilder   Builder
	valueSchemaBuilder Builder
}

func NewSchemaBuilder(
	schemaType Type,
) Builder {

	return &schemaBuilderImpl{
		schemaType: schemaType,
		index:      -1,
	}
}

func (s *schemaBuilderImpl) SchemaType() Type {
	return s.schemaType
}

func (s *schemaBuilderImpl) FieldName(
	fieldName string,
) Builder {

	s.fieldName = fieldName
	return s
}

func (s *schemaBuilderImpl) GetFieldName() string {
	return s.fieldName
}

func (s *schemaBuilderImpl) SchemaName(
	schemaName string,
) Builder {

	s.schemaName = schemaName
	return s
}

func (s *schemaBuilderImpl) GetSchemaName() string {
	return s.schemaName
}

func (s *schemaBuilderImpl) Optional() Builder {
	s.optional = true
	return s
}

func (s *schemaBuilderImpl) SetOptional(
	optional bool,
) Builder {

	s.optional = optional
	return s
}

func (s *schemaBuilderImpl) Required() Builder {
	s.optional = false
	return s
}

func (s *schemaBuilderImpl) IsOptional() bool {
	return s.optional
}

func (s *schemaBuilderImpl) DefaultValue(
	defaultValue *string,
) Builder {

	s.defaultValue = defaultValue
	return s
}

func (s *schemaBuilderImpl) GetDefaultValue() *string {
	return s.defaultValue
}

func (s *schemaBuilderImpl) Version(
	version int,
) Builder {

	s.version = version
	return s
}

func (s *schemaBuilderImpl) GetVersion() int {
	return s.version
}

func (s *schemaBuilderImpl) Index(
	index int,
) Builder {

	s.index = index
	return s
}

func (s *schemaBuilderImpl) GetIndex() int {
	return s.index
}

func (s *schemaBuilderImpl) Documentation(
	documentation *string,
) Builder {

	s.documentation = documentation
	return s
}

func (s *schemaBuilderImpl) GetDocumentation() *string {
	return s.documentation
}

func (s *schemaBuilderImpl) Parameter(
	key string, value any,
) Builder {

	if s.parameters == nil {
		s.parameters = make(map[string]any)
	}
	s.parameters[key] = value
	return s
}

func (s *schemaBuilderImpl) Parameters() map[string]any {
	return s.parameters
}

func (s *schemaBuilderImpl) Field(
	name FieldName, index int, schemaBuilder Builder,
) Builder {

	if s.fields == nil {
		s.fields = make(map[string]Field)
	}
	s.fields[name] = &fieldImpl{
		name:          name,
		index:         index,
		schemaBuilder: schemaBuilder.Clone().FieldName(name).Index(index),
	}
	return s
}

func (s *schemaBuilderImpl) Fields() map[string]Field {
	return s.fields
}

func (s *schemaBuilderImpl) KeySchema(
	builder Builder,
) Builder {

	s.keySchemaBuilder = builder
	return s
}

func (s *schemaBuilderImpl) GetKeySchema() Builder {
	return s.keySchemaBuilder
}

func (s *schemaBuilderImpl) ValueSchema(
	builder Builder,
) Builder {

	s.valueSchemaBuilder = builder
	return s
}

func (s *schemaBuilderImpl) GetValueSchema() Builder {
	return s.valueSchemaBuilder
}

func (s *schemaBuilderImpl) Clone() Builder {
	return &schemaBuilderImpl{
		fieldName:          s.fieldName,
		schemaName:         s.schemaName,
		schemaType:         s.schemaType,
		version:            s.version,
		optional:           s.optional,
		defaultValue:       s.defaultValue,
		documentation:      s.documentation,
		parameters:         s.parameters,
		fields:             s.fields,
		keySchemaBuilder:   s.keySchemaBuilder,
		valueSchemaBuilder: s.valueSchemaBuilder,
	}
}

func (s *schemaBuilderImpl) Build() Struct {
	schemaStruct := Struct{
		FieldNameType: s.schemaType,
	}
	switch s.schemaType {
	case ARRAY:
		schemaStruct[FieldNameValueSchema] = s.valueSchemaBuilder.Build()
	case MAP:
		schemaStruct[FieldNameKeySchema] = s.keySchemaBuilder.Build()
		schemaStruct[FieldNameValueSchema] = s.valueSchemaBuilder.Build()
	case STRUCT:
		fields := functional.Sort(lo.Values(s.fields), func(this, other Field) bool {
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
