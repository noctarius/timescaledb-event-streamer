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

package typemanager

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/functional"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
)

type pgType struct {
	namespace  string
	name       string
	kind       pgtypes.PgKind
	oid        uint32
	category   pgtypes.PgCategory
	arrayType  bool
	recordType bool
	oidArray   uint32
	oidElement uint32
	oidParent  uint32
	modifiers  int
	enumValues []string
	delimiter  string
	schemaType schema.Type

	typeManager         *typeManager
	schemaBuilder       schema.Builder
	resolvedArrayType   pgtypes.PgType
	resolvedElementType pgtypes.PgType
	resolvedParentType  pgtypes.PgType
}

func newType(
	typeManager *typeManager, namespace, name string, kind pgtypes.PgKind, oid uint32,
	category pgtypes.PgCategory, arrayType, recordType bool, oidArray, oidElement, oidParent uint32,
	modifiers int, enumValues []string, delimiter string,
) pgtypes.PgType {

	t := &pgType{
		namespace:   namespace,
		name:        name,
		kind:        kind,
		oid:         oid,
		category:    category,
		arrayType:   arrayType,
		recordType:  recordType,
		oidArray:    oidArray,
		oidElement:  oidElement,
		oidParent:   oidParent,
		modifiers:   modifiers,
		enumValues:  enumValues,
		delimiter:   delimiter,
		typeManager: typeManager,
		schemaType:  typeManager.getSchemaType(oid, arrayType, kind),
	}

	// If this is a primitive type we can resolve early
	if t.schemaType.IsPrimitive() {
		t.schemaBuilder = typeManager.resolveSchemaBuilder(t)
	}

	return t
}

func (t *pgType) Namespace() string {
	return t.namespace
}

func (t *pgType) Name() string {
	return t.name
}

func (t *pgType) Kind() pgtypes.PgKind {
	return t.kind
}

func (t *pgType) Oid() uint32 {
	return t.oid
}

func (t *pgType) Category() pgtypes.PgCategory {
	return t.category
}

func (t *pgType) IsArray() bool {
	return t.arrayType
}

func (t *pgType) IsRecord() bool {
	return t.recordType
}

func (t *pgType) ArrayType() pgtypes.PgType {
	if t.resolvedArrayType == nil {
		arrayType, err := t.typeManager.ResolveDataType(t.oidArray)
		if err != nil {
			panic(err)
		}
		t.resolvedArrayType = arrayType
	}
	return t.resolvedArrayType
}

func (t *pgType) ElementType() pgtypes.PgType {
	if t.resolvedElementType == nil {
		elementType, err := t.typeManager.ResolveDataType(t.oidElement)
		if err != nil {
			panic(err)
		}
		t.resolvedElementType = elementType
	}
	return t.resolvedElementType
}

func (t *pgType) ParentType() pgtypes.PgType {
	if t.resolvedParentType == nil {
		parentType, err := t.typeManager.ResolveDataType(t.oidParent)
		if err != nil {
			panic(err)
		}
		t.resolvedParentType = parentType
	}
	return t.resolvedParentType
}

func (t *pgType) OidArray() uint32 {
	return t.oidArray
}

func (t *pgType) OidElement() uint32 {
	return t.oidElement
}

func (t *pgType) OidParent() uint32 {
	return t.oidParent
}

func (t *pgType) Modifiers() int {
	return t.modifiers
}

func (t *pgType) EnumValues() []string {
	if t.enumValues == nil {
		return []string{}
	}
	enumValues := make([]string, 0, len(t.enumValues))
	copy(enumValues, t.enumValues)
	return enumValues
}

func (t *pgType) Delimiter() string {
	return t.delimiter
}

func (t *pgType) SchemaType() schema.Type {
	return t.schemaType
}

func (t *pgType) SchemaBuilder() schema.Builder {
	if t.schemaBuilder == nil {
		t.schemaBuilder = t.typeManager.resolveSchemaBuilder(t)
	}
	return t.schemaBuilder.Clone()
}

func (t *pgType) Format() string {
	if t.IsArray() {
		return fmt.Sprintf("%s[]", t.ElementType().Format())
	}
	return t.Name()
}

func (t *pgType) Equal(
	other pgtypes.PgType,
) bool {

	return t.namespace == other.Namespace() &&
		t.name == other.Name() &&
		t.kind == other.Kind() &&
		t.oid == other.Oid() &&
		t.category == other.Category() &&
		t.arrayType == other.IsArray() &&
		t.oidArray == other.OidArray() &&
		t.oidElement == other.OidElement() &&
		t.recordType == other.IsRecord() &&
		t.oidParent == other.OidParent() &&
		t.modifiers == other.Modifiers() &&
		t.delimiter == other.Delimiter() &&
		t.schemaType == other.SchemaType() &&
		functional.ArrayEqual(t.enumValues, other.EnumValues())
}
