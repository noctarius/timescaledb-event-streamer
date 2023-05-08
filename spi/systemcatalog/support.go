package systemcatalog

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"strings"
)

// IsHypertableEvent returns true if the given relation represents
// a hypertable system catalog entry, otherwise false
func IsHypertableEvent(relation *pgtypes.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "hypertable"
}

// IsChunkEvent returns true if the given relation represents
// a chunk system catalog entry, otherwise false
func IsChunkEvent(relation *pgtypes.RelationMessage) bool {
	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "chunk"
}

// IsContinuousAggregateHypertable returns true if the given
// hypertable name is a backing hypertable for a continuous
// aggregate, otherwise false
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
