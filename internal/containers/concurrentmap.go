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
