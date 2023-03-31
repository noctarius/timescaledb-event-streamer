package model

import "fmt"

type Column struct {
	name         string
	dataType     uint32
	typeName     string
	nullable     bool
	primaryKey   bool
	defaultValue *string
}

func NewColumn(name string, dataType uint32, typeName string, nullable, primaryKey bool, defaultValue *string) Column {
	return Column{
		name:         name,
		dataType:     dataType,
		typeName:     typeName,
		nullable:     nullable,
		primaryKey:   primaryKey,
		defaultValue: defaultValue,
	}
}

func (c Column) Name() string {
	return c.name
}

func (c Column) DataType() uint32 {
	return c.dataType
}

func (c Column) TypeName() string {
	return c.typeName
}

func (c Column) Nullable() bool {
	return c.nullable
}

func (c Column) PrimaryKey() bool {
	return c.primaryKey
}

func (c Column) DefaultValue() *string {
	return c.defaultValue
}

func (c Column) equals(other Column) bool {
	return c.name == other.name &&
		c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue))
}

func (c Column) equalsExceptName(other Column) bool {
	return c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.primaryKey == other.primaryKey &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue))
}

func (c Column) differences(new Column) map[string]string {
	differences := make(map[string]string, 0)
	if c.name != new.name {
		differences["name"] = fmt.Sprintf("%s=>%s", c.name, new.name)
	}
	if c.dataType != new.dataType {
		differences["dataType"] = fmt.Sprintf("%d=>%d", c.dataType, new.dataType)
	}
	if c.typeName != new.typeName {
		differences["typeName"] = fmt.Sprintf("%s=>%s", c.typeName, new.typeName)
	}
	if c.nullable != new.nullable {
		differences["nullable"] = fmt.Sprintf("%t=>%t", c.nullable, new.nullable)
	}
	if c.primaryKey != new.primaryKey {
		differences["primaryKey"] = fmt.Sprintf("%t=>%t", c.primaryKey, new.primaryKey)
	}
	if (c.defaultValue == nil && new.defaultValue != nil) ||
		(c.defaultValue != nil && new.defaultValue == nil) {
		o := "<nil>"
		if c.defaultValue != nil {
			o = fmt.Sprintf("%s", *c.defaultValue)
		}
		n := "<nil>"
		if new.defaultValue != nil {
			n = fmt.Sprintf("%s", *new.defaultValue)
		}
		differences["defaultValue"] = fmt.Sprintf("%s=>%s", o, n)
	}
	return differences
}
