package model

import "testing"

func TestSchemaDifferences_Added_Column(t *testing.T) {
	expected := "added: {name:test4 dataType:10 typeName:foo nullable:false identity:false defaultValue:<nil>}"
	oldColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
		NewColumn("test3", 10, "foo", false, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
		NewColumn("test3", 10, "foo", false, false, nil),
		NewColumn("test4", 10, "foo", false, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false)
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
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
		NewColumn("test3", 10, "foo", false, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test4", 10, "foo", false, false, nil),
		NewColumn("test3", 10, "foo", false, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false)
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
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
		NewColumn("test3", 10, "foo", false, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
		NewColumn("test4", 10, "foo", false, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false)
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
	expected := "dropped: {name:test2 dataType:11 typeName:foo nullable:false identity:false defaultValue:<nil>}"
	oldColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 11, "foo", false, false, nil),
		NewColumn("test3", 12, "foo", false, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test3", 12, "foo", false, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false)
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
	expected := "dropped: {name:test3 dataType:10 typeName:foo nullable:false identity:false defaultValue:<nil>}"
	oldColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
		NewColumn("test3", 10, "foo", false, false, nil),
	}
	newColumns := []Column{
		NewColumn("test1", 10, "foo", false, false, nil),
		NewColumn("test2", 10, "foo", false, false, nil),
	}
	hypertable := NewHypertable(1, "", "", "", "", "", nil, 0, false)
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
