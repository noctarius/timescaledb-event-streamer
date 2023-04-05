package model

import (
	"github.com/jackc/pglogrepl"
	"strings"
)

func IsHypertableEvent(relation *pglogrepl.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "hypertable"
}

func IsChunkEvent(relation *pglogrepl.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "chunk"
}

func IsContinuousAggregateHypertable(hypertableName string) bool {
	return strings.HasPrefix(hypertableName, "_materialized_")
}

func min(i, o int) int {
	if i < o {
		return i
	}
	return o
}

func isContinuousAggregate(hypertableName string, viewSchema, viewName *string) bool {
	return IsContinuousAggregateHypertable(hypertableName) && viewSchema != nil && viewName != nil
}
