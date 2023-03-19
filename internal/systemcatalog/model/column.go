package model

type Column struct {
	name         string
	dataType     uint32
	typeName     string
	nullable     bool
	identity     bool
	defaultValue *string
}

func NewColumn(name string, dataType uint32, typeName string, nullable, identity bool, defaultValue *string) Column {
	return Column{
		name:         name,
		dataType:     dataType,
		typeName:     typeName,
		nullable:     nullable,
		identity:     identity,
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

func (c Column) Identify() bool {
	return c.identity
}

func (c Column) DefaultValue() *string {
	return c.defaultValue
}

func (c Column) equals(other Column) bool {
	return c.name == other.name &&
		c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.identity == other.identity &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue))
}

func (c Column) equalsExceptName(other Column) bool {
	return c.typeName == other.typeName &&
		c.dataType == other.dataType &&
		c.nullable == other.nullable &&
		c.identity == other.identity &&
		((c.defaultValue == nil && other.defaultValue == nil) ||
			(c.defaultValue != nil && other.defaultValue != nil && *c.defaultValue == *other.defaultValue))
}

func (c Column) differences(new Column) map[string]string {
	differences := make(map[string]string, 0)
	// TODO
	return differences
}
