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

package systemcatalog

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"testing"
)

var fooType = &testPgType{}

func TestSchemaDifferences_Added_Column(t *testing.T) {
	expected := "added: {name:test4 dataType:10 modifiers:-1 pgType:foo nullable:false primaryKey:false keySeq:<nil> indexName:<nil> replicaIdent:false defaultValue:<nil> dimension:false dimAligned:false dimType:<nil> dimSeq:<nil>maxCharLength:<nil>}"
	oldColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
		NewColumn("test3", 10, -1, fooType, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
		NewColumn("test3", 10, -1, fooType, false, nil),
		NewColumn("test4", 10, -1, fooType, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)
	hypertable.ApplyTableSchema(oldColumns)
	differences := hypertable.ApplyTableSchema(newColumns)

	if len(differences) != 1 {
		t.Fatalf("should have 1 difference but got 0")
	}

	if d, ok := differences["test4"]; ok {
		if d == expected {
			return
		}
		t.Fatalf("change was supposed to be '%s' but was '%s'", expected, d)
	}
	t.Fatalf("should have a difference for key 'test4' but doesn't")
}

func TestSchemaDifferences_Renamed_Column(t *testing.T) {
	expected := "name:test2=>test4"
	oldColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
		NewColumn("test3", 10, -1, fooType, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test4", 10, -1, fooType, false, nil),
		NewColumn("test3", 10, -1, fooType, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)
	hypertable.ApplyTableSchema(oldColumns)
	differences := hypertable.ApplyTableSchema(newColumns)

	if len(differences) != 1 {
		t.Fatalf("should have 1 difference but got 0")
	}

	if d, ok := differences["test4"]; ok {
		if d == expected {
			return
		}
		t.Fatalf("change was supposed to be '%s' but was '%s'", expected, d)
	}
	t.Fatalf("should have a difference for key 'test4' but doesn't")
}

func TestSchemaDifferences_Renamed_Last_Column(t *testing.T) {
	expected := "name:test3=>test4"
	oldColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
		NewColumn("test3", 10, -1, fooType, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
		NewColumn("test4", 10, -1, fooType, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)
	hypertable.ApplyTableSchema(oldColumns)
	differences := hypertable.ApplyTableSchema(newColumns)

	if len(differences) != 1 {
		t.Fatalf("should have 1 difference but got 0")
	}

	if d, ok := differences["test3"]; ok {
		if d == expected {
			return
		}
		t.Fatalf("change was supposed to be '%s' but was '%s'", expected, d)
	}
	t.Fatalf("should have a difference for key 'test3' but doesn't")
}

func TestSchemaDifferences_Dropped_Column(t *testing.T) {
	expected := "dropped: {name:test2 dataType:11 modifiers:-1 pgType:foo nullable:false primaryKey:false keySeq:<nil> indexName:<nil> replicaIdent:false defaultValue:<nil> dimension:false dimAligned:false dimType:<nil> dimSeq:<nil>maxCharLength:<nil>}"
	oldColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 11, -1, fooType, false, nil),
		NewColumn("test3", 12, -1, fooType, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test3", 12, -1, fooType, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)
	hypertable.ApplyTableSchema(oldColumns)
	differences := hypertable.ApplyTableSchema(newColumns)

	if len(differences) != 1 {
		t.Fatalf("should have 1 difference but got 0")
	}

	if d, ok := differences["test2"]; ok {
		if d == expected {
			return
		}
		t.Fatalf("change was supposed to be '%s' but was '%s'", expected, d)
	}
	t.Fatalf("should have a difference for key 'test2' but doesn't")
}

func TestSchemaDifferences_Dropped_Last_Column(t *testing.T) {
	expected := "dropped: {name:test3 dataType:10 modifiers:-1 pgType:foo nullable:false primaryKey:false keySeq:<nil> indexName:<nil> replicaIdent:false defaultValue:<nil> dimension:false dimAligned:false dimType:<nil> dimSeq:<nil>maxCharLength:<nil>}"
	oldColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
		NewColumn("test3", 10, -1, fooType, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, -1, fooType, false, nil),
		NewColumn("test2", 10, -1, fooType, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)
	hypertable.ApplyTableSchema(oldColumns)
	differences := hypertable.ApplyTableSchema(newColumns)

	if len(differences) != 1 {
		t.Fatalf("should have 1 difference but got 0")
	}

	if d, ok := differences["test3"]; ok {
		if d == expected {
			return
		}
		t.Fatalf("change was supposed to be '%s' but was '%s'", expected, d)
	}
	t.Fatalf("should have a difference for key 'test3' but doesn't")
}

type testPgType struct {
}

func (t *testPgType) Namespace() string {
	return "namespace"
}

func (t *testPgType) Name() string {
	return "foo"
}

func (t *testPgType) Kind() pgtypes.PgKind {
	return pgtypes.BaseKind
}

func (t *testPgType) Oid() uint32 {
	return 123
}

func (t *testPgType) Category() pgtypes.PgCategory {
	return pgtypes.Numeric
}

func (t *testPgType) IsArray() bool {
	return false
}

func (t *testPgType) IsRecord() bool {
	return false
}

func (t *testPgType) ArrayType() pgtypes.PgType {
	return nil
}

func (t *testPgType) ElementType() pgtypes.PgType {
	return nil
}

func (t *testPgType) ParentType() pgtypes.PgType {
	return nil
}

func (t *testPgType) OidArray() uint32 {
	return 0
}

func (t *testPgType) OidElement() uint32 {
	return 0
}

func (t *testPgType) OidParent() uint32 {
	return 0
}

func (t *testPgType) Modifiers() int {
	return -1
}

func (t *testPgType) EnumValues() []string {
	return nil
}

func (t *testPgType) Delimiter() string {
	return ","
}

func (t *testPgType) SchemaType() schema.Type {
	return schema.INT16
}

func (t *testPgType) SchemaBuilder() schema.SchemaBuilder {
	return nil
}

func (t *testPgType) Format() string {
	return ""
}

func (t *testPgType) Equal(_ pgtypes.PgType) bool {
	return true
}
