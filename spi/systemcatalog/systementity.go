package systemcatalog

type SystemEntity interface {
	SchemaName() string
	TableName() string
	CanonicalName() string
}

type baseSystemEntity struct {
	schemaName string
	tableName  string
}

func NewSystemEntity(schemaName, tableName string) SystemEntity {
	return &baseSystemEntity{
		schemaName: schemaName,
		tableName:  tableName,
	}
}

func (bse *baseSystemEntity) SchemaName() string {
	return bse.schemaName
}

func (bse *baseSystemEntity) TableName() string {
	return bse.tableName
}

func (bse *baseSystemEntity) CanonicalName() string {
	return MakeRelationKey(bse.schemaName, bse.tableName)
}
