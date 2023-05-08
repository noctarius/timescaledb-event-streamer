package systemcatalog

// SystemEntity represents an entity defined by
// its canonical elements (schema and table names)
type SystemEntity interface {
	// SchemaName returns the schema name of the entity
	SchemaName() string
	// TableName returns the table name of the entity
	TableName() string
	// CanonicalName returns the canonical name of the entity >>schema.table<<
	CanonicalName() string
}

type baseSystemEntity struct {
	schemaName string
	tableName  string
}

// NewSystemEntity instantiates a new basic SystemEntity
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
