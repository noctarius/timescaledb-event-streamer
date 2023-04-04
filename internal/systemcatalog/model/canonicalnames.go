package model

import "fmt"

func canonicalChunkTablePrefix(hypertable *Hypertable) string {
	return MakeRelationKey(hypertable.AssociatedSchemaName(), hypertable.AssociatedTablePrefix())
}

func canonicalHypertableName(hypertable *Hypertable) string {
	return MakeRelationKey(hypertable.SchemaName(), hypertable.HypertableName())
}

func canonicalContinuousAggregateName(hypertable *Hypertable) string {
	return MakeRelationKey(*hypertable.viewSchema, *hypertable.viewName)
}

func canonicalChunkName(chunk *Chunk) string {
	return MakeRelationKey(chunk.SchemaName(), chunk.TableName())
}

func MakeRelationKey(schemaName, tableName string) string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
}
