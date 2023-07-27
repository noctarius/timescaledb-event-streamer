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
	"sync"
)

type ConcurrentMap[K comparable, V any] struct {
	m *sync.Map
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		m: &sync.Map{},
	}
}

func (m *ConcurrentMap[K, V]) Load(key K) (value V, ok bool) {
	v, ok := m.m.Load(key)
	if !ok {
		return functional.Zero[V](), false
	}
	return v.(V), ok
}

func (m *ConcurrentMap[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

func (m *ConcurrentMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, ok := m.m.LoadOrStore(key, value)
	return v.(V), ok
}

func (m *ConcurrentMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, ok := m.m.LoadAndDelete(key)
	return v.(V), ok
}

func (m *ConcurrentMap[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *ConcurrentMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	v, ok := m.m.Swap(key, value)
	return v.(V), ok
}

func (m *ConcurrentMap[K, V]) CompareAndSwap(key K, old V, new V) bool {
	return m.m.CompareAndSwap(key, old, new)
}

func (m *ConcurrentMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	return m.m.CompareAndDelete(key, old)
}

func (m *ConcurrentMap[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
