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

package containers

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/functional"
)

const (
	relationCacheBaseSize       = 0
	relationCacheSizeMultiplier = 1.5
)

type RelationCache[V any] struct {
	cache      []*V
	empty      V
	lowerBound uint32
	upperBound uint32
}

func NewRelationCache[V any]() *RelationCache[V] {
	return &RelationCache[V]{
		cache:      make([]*V, relationCacheBaseSize),
		empty:      functional.Zero[V](),
		lowerBound: 0,
		upperBound: 0,
	}
}

func (rc *RelationCache[V]) Get(
	oid uint32,
) (value V, present bool) {

	if oid > rc.upperBound {
		return rc.empty, false
	}

	if v := rc.cache[rc.location(oid, rc.lowerBound)]; v != nil {
		return *v, true
	}
	return rc.empty, false
}

func (rc *RelationCache[V]) Set(
	oid uint32, value V,
) {

	oldLowerBound := rc.lowerBound

	needsResizing := false
	if rc.lowerBound == 0 || rc.lowerBound > oid {
		rc.lowerBound = oid
		needsResizing = true
	}
	if rc.upperBound < oid {
		rc.upperBound = oid
		needsResizing = true
	}

	if needsResizing {
		newCacheSize := uint32(float64(rc.upperBound-rc.lowerBound+1) * relationCacheSizeMultiplier)
		newCache := make([]*V, newCacheSize)

		target := newCache
		source := rc.cache
		if oldLowerBound > rc.lowerBound {
			diff := oldLowerBound - rc.lowerBound
			target = target[diff:]
		}
		copy(target, source)

		rc.cache = newCache
	}
	rc.cache[rc.location(oid, rc.lowerBound)] = &value
}

func (rc *RelationCache[V]) location(
	oid, lowerBound uint32,
) uint32 {

	return oid - lowerBound
}
