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
	"github.com/go-errors/errors"
	"github.com/gookit/goutil/reflects"
	"github.com/noctarius/timescaledb-event-streamer/internal/functional"
	"reflect"
	"sync/atomic"
)

type CasCache[K comparable, V any] struct {
	mapPtr atomic.Pointer[map[K]V]
}

func NewCasCache[K comparable, V any]() *CasCache[K, V] {
	return &CasCache[K, V]{
		mapPtr: atomic.Pointer[map[K]V]{},
	}
}

func (cc *CasCache[K, V]) Get(
	key K,
) (value V, ok bool) {

	m := cc.mapPtr.Load()
	if m == nil {
		return functional.Zero[V](), false
	}

	value, ok = (*m)[key]
	return
}

func (cc *CasCache[K, V]) GetOrCompute(
	key K, producer func() (V, error),
) (V, error) {

	m := cc.mapPtr.Load()
	if m != nil {
		v, present := (*m)[key]
		// If available return immediately
		if present {
			return v, nil
		}
	}

	// Couldn't find it, so let's produce it
	v, err := producer()
	if err != nil {
		return functional.Zero[V](), err
	}

	for {
		// Reload, since it may have been computed and stored concurrently
		m = cc.mapPtr.Load()

		// Retest for existing value
		if m != nil {
			v, present := (*m)[key]
			if present {
				return v, nil
			}
		}

		// Create new map and copy data
		n := map[K]V{key: v}
		if m != nil {
			for k, v := range *m {
				n[k] = v
			}
		}

		// Try exchange
		if cc.mapPtr.CompareAndSwap(m, &n) {
			return v, nil
		}
	}
}

func (cc *CasCache[K, V]) Set(
	key K, value V,
) {

	for {
		o := cc.mapPtr.Load()
		n := map[K]V{key: value}

		// Copy data
		if o != nil {
			for k, v := range *o {
				n[k] = v
			}
		}

		// Try exchange
		if cc.mapPtr.CompareAndSwap(o, &n) {
			break
		}
	}
}

func (cc *CasCache[K, V]) SetAll(
	m map[K]V,
) {

	for {
		o := cc.mapPtr.Load()

		// Copy data from existing map
		if o != nil {
			for k, v := range *o {
				m[k] = v
			}
		}

		// Try exchange
		if cc.mapPtr.CompareAndSwap(o, &m) {
			break
		}
	}
}

func (cc *CasCache[K, V]) TransformSetAndGet(
	key K, transformer func(old V) (V, error),
) (V, error) {

	m := cc.mapPtr.Load()
	if m == nil {
		return functional.Zero[V](), errors.Errorf("CasCache not initialized yet")
	}

	for {
		// Reload, since it may have been transformed and stored concurrently
		m = cc.mapPtr.Load()

		v, present := (*m)[key]
		if !present {
			return functional.Zero[V](), errors.Errorf(
				"Key %v not present", reflects.String(reflect.ValueOf(key)),
			)
		}

		vnew, err := transformer(v)
		if err != nil {
			return functional.Zero[V](), err
		}

		// Create new map and copy data
		n := make(map[K]V)
		for k, v := range *m {
			n[k] = v
		}
		n[key] = vnew

		// Try exchange
		if cc.mapPtr.CompareAndSwap(m, &n) {
			return vnew, nil
		}
	}
}

func (cc *CasCache[K, V]) Length() int {
	m := cc.mapPtr.Load()
	if m == nil {
		return 0
	}
	return len(*m)
}
