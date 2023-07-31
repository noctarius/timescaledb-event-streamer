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

package pgtypes

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/stretchr/testify/mock"
)

type mockPgType struct {
	mock.Mock
}

func (t *mockPgType) CompositeColumns() ([]CompositeColumn, error) {
	args := t.Called()
	return args.Get(0).([]CompositeColumn), args.Error(1)
}

func (t *mockPgType) SchemaBuilder() schema.Builder {
	args := t.Called()
	return args.Get(0).(schema.Builder)
}

func (t *mockPgType) Namespace() string {
	args := t.Called()
	return args.String(0)
}

func (t *mockPgType) Name() string {
	args := t.Called()
	return args.String(0)
}

func (t *mockPgType) Kind() PgKind {
	args := t.Called()
	return args.Get(0).(PgKind)
}

func (t *mockPgType) Oid() uint32 {
	args := t.Called()
	return args.Get(0).(uint32)
}

func (t *mockPgType) Category() PgCategory {
	args := t.Called()
	return args.Get(0).(PgCategory)
}

func (t *mockPgType) IsArray() bool {
	args := t.Called()
	return args.Bool(0)
}

func (t *mockPgType) IsRecord() bool {
	args := t.Called()
	return args.Bool(0)
}

func (t *mockPgType) ArrayType() PgType {
	args := t.Called()
	return args.Get(0).(PgType)
}

func (t *mockPgType) ElementType() PgType {
	args := t.Called()
	return args.Get(0).(PgType)
}

func (t *mockPgType) BaseType() PgType {
	args := t.Called()
	return args.Get(0).(PgType)
}

func (t *mockPgType) OidArray() uint32 {
	args := t.Called()
	return args.Get(0).(uint32)
}

func (t *mockPgType) OidElement() uint32 {
	args := t.Called()
	return args.Get(0).(uint32)
}

func (t *mockPgType) OidBase() uint32 {
	args := t.Called()
	return args.Get(0).(uint32)
}

func (t *mockPgType) Modifiers() int {
	args := t.Called()
	return args.Int(0)
}

func (t *mockPgType) EnumValues() []string {
	args := t.Called()
	return args.Get(0).([]string)
}

func (t *mockPgType) Delimiter() string {
	args := t.Called()
	return args.String(0)
}

func (t *mockPgType) SchemaType() schema.Type {
	args := t.Called()
	return args.Get(0).(schema.Type)
}

func (t *mockPgType) Format() string {
	args := t.Called()
	return args.String(0)
}

func (t *mockPgType) Equal(
	other PgType,
) bool {

	args := t.Called(other)
	return args.Bool(0)
}
