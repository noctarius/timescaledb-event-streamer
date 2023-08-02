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

import "github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"

const (
	relationCacheBaseSize       = 300000
	relationCacheSizeMultiplier = 1.5
)

type RelationCache struct {
	cache     []*pgtypes.RelationMessage
	cacheSize uint32
}

func NewRelationCache() *RelationCache {
	return &RelationCache{
		cache:     make([]*pgtypes.RelationMessage, relationCacheBaseSize),
		cacheSize: relationCacheBaseSize,
	}
}

func (rc *RelationCache) Get(
	oid uint32,
) (msg *pgtypes.RelationMessage, present bool) {

	if oid >= rc.cacheSize {
		return nil, false
	}

	if msg = rc.cache[oid]; msg != nil {
		return msg, true
	}
	return nil, false
}

func (rc *RelationCache) Set(
	oid uint32, msg *pgtypes.RelationMessage,
) {

	if oid >= rc.cacheSize {
		newCacheSize := uint32(float64(rc.cacheSize) * relationCacheSizeMultiplier)
		newCache := make([]*pgtypes.RelationMessage, newCacheSize)
		copy(newCache, rc.cache)
		rc.cache = newCache
	}
	rc.cache[oid] = msg
}
