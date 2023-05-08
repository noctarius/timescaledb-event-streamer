package systemcatalog

import "fmt"

func canonicalContinuousAggregateName(hypertable *Hypertable) string {
	return MakeRelationKey(*hypertable.viewSchema, *hypertable.viewName)
}

func MakeRelationKey(schemaName, tableName string) string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
}
