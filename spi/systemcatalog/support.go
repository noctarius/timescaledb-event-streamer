/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package systemcatalog

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"strings"
)

// IsHypertableEvent returns true if the given relation represents
// a hypertable system catalog entry, otherwise false
func IsHypertableEvent(
	relation *pgtypes.RelationMessage,
) bool {

	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "hypertable"
}

// IsChunkEvent returns true if the given relation represents
// a chunk system catalog entry, otherwise false
func IsChunkEvent(
	relation *pgtypes.RelationMessage,
) bool {

	return relation.Namespace == "_timescaledb_catalog" && relation.RelationName == "chunk"
}

func IsVanillaTable(
	relation *pgtypes.RelationMessage,
) bool {

	return relation.Namespace != "_timescaledb_catalog" && relation.Namespace != "_timescaledb_internal"
}

// IsContinuousAggregateHypertable returns true if the given
// hypertable name is a backing hypertable for a continuous
// aggregate, otherwise false
func IsContinuousAggregateHypertable(
	hypertableName string,
) bool {

	return strings.HasPrefix(hypertableName, "_materialized_")
}

func isContinuousAggregate(
	hypertableName string, viewSchema, viewName *string,
) bool {

	return IsContinuousAggregateHypertable(hypertableName) && viewSchema != nil && viewName != nil
}
