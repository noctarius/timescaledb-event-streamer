package model

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/pg/decoding"
	"strings"
)

func IsHypertableEvent(relation *decoding.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "hypertable"
}

func IsChunkEvent(relation *decoding.RelationMessage) bool {
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
