package systemcatalog

import "fmt"

func canonicalChunkTablePrefix(hypertable *Hypertable) string {
	return MakeRelationKey(hypertable.AssociatedSchemaName(), hypertable.AssociatedTablePrefix())
}

func canonicalContinuousAggregateName(hypertable *Hypertable) string {
	return MakeRelationKey(*hypertable.viewSchema, *hypertable.viewName)
}

func MakeRelationKey(schemaName, tableName string) string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
}
