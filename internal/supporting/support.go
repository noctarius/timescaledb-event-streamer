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

package supporting

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

var validCharacters = []string{
	"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
	"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
}

type Iterator[E any] func() (E, bool)

func MapWithIterator[E, R any](iterator Iterator[E], mapper func(element E) R) []R {
	result := make([]R, 0)
	for {
		if e, present := iterator(); !present {
			break
		} else {
			result = append(result, mapper(e))
		}
	}
	return result
}

func Map[E, R any](elements []E, mapper func(element E) R) []R {
	result := make([]R, 0, len(elements))
	for _, e := range elements {
		result = append(result, mapper(e))
	}
	return result
}

func MapMapper[K comparable, V, R any](elements map[K]V, mapper func(key K, element V) R) []R {
	result := make([]R, 0, len(elements))
	for k, e := range elements {
		result = append(result, mapper(k, e))
	}
	return result
}

func AddrOf[T any](value T) *T {
	return &value
}

func Sort[I any](collection []I, less func(this, other I) bool) {
	sort.Slice(collection, func(i, j int) bool {
		return less(collection[i], collection[j])
	})
}

func IndexOfWithMatcher[I any](collection []I, matcher func(other I) bool) int {
	for i, candidate := range collection {
		if matcher(candidate) {
			return i
		}
	}
	return -1
}

func IndexOf[I comparable](collection []I, item I) int {
	for i, candidate := range collection {
		if candidate == item {
			return i
		}
	}
	return -1
}

func ContainsWithMatcher[I any](collection []I, matcher func(other I) bool) bool {
	return IndexOfWithMatcher(collection, matcher) != -1
}

func Contains[I comparable](collection []I, item I) bool {
	return IndexOf(collection, item) != -1
}

func Filter[I any](collection []I, filter func(item I) bool) []I {
	result := make([]I, 0)
	for _, candidate := range collection {
		if filter(candidate) {
			result = append(result, candidate)
		}
	}
	return result
}

func FilterMap[K, V comparable](source map[K]V, filter func(key K, value V) bool) map[K]V {
	result := make(map[K]V)
	for key, value := range source {
		if filter(key, value) {
			result[key] = value
		}
	}
	return result
}

func RandomTextString(length int) string {
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		index := rand.Intn(len(validCharacters))
		builder.WriteString(validCharacters[index])
	}
	return builder.String()
}

func RandomNumber(min, max int) int {
	return min + rand.Intn(max-min)
}

type Numbers interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

func Min[T Numbers](i, o T) T {
	if i < o {
		return i
	}
	return o
}

func DistinctItems[V any](items []V, identityFn func(item V) string) []V {
	distincting := make(map[string]V, 0)
	for _, item := range items {
		identity := identityFn(item)
		distincting[identity] = item
	}
	distinctItems := make([]V, 0, len(distincting))
	for _, item := range distincting {
		distinctItems = append(distinctItems, item)
	}
	return distinctItems
}

var ErrWaiterTimeout = fmt.Errorf("waiter timed out")

type Waiter struct {
	done    chan bool
	timer   *time.Timer
	timeout time.Duration
}

func NewWaiter() *Waiter {
	return &Waiter{
		done: make(chan bool, 1),
	}
}

func NewWaiterWithTimeout(timeout time.Duration) *Waiter {
	return &Waiter{
		done:    make(chan bool, 1),
		timer:   time.NewTimer(timeout),
		timeout: timeout,
	}
}

func (w *Waiter) Reset() {
	if w.timer != nil {
		w.timer.Stop()
		// Make sure channel is drained
		select {
		case <-w.timer.C:
		default:
		}
		w.timer = time.NewTimer(w.timeout)
	}
}

func (w *Waiter) Signal() {
	w.done <- true
}

func (w *Waiter) Await() error {
	if w.timer == nil {
		<-w.done
		return nil
	}

	select {
	case <-w.done:
		w.timer.Stop()
		// Make sure channel is drained
		select {
		case <-w.timer.C:
		default:
		}
		return nil
	case <-w.timer.C:
		return ErrWaiterTimeout
	}
}

type ShutdownAwaiter struct {
	start *Waiter
	done  *Waiter
}

func NewShutdownAwaiter() *ShutdownAwaiter {
	return &ShutdownAwaiter{
		start: NewWaiter(),
		done:  NewWaiter(),
	}
}

func (sa *ShutdownAwaiter) SignalShutdown() {
	sa.start.Signal()
}

func (sa *ShutdownAwaiter) AwaitShutdown() error {
	return sa.start.Await()
}

func (sa *ShutdownAwaiter) AwaitShutdownChan() <-chan bool {
	return sa.start.done
}

func (sa *ShutdownAwaiter) SignalDone() {
	sa.done.Signal()
}

func (sa *ShutdownAwaiter) AwaitDone() error {
	return sa.done.Await()
}

type MultiShutdownAwaiter struct {
	slots     uint
	starters  []*Waiter
	doneGroup sync.WaitGroup
}

func NewMultiShutdownAwaiter(slots uint) *MultiShutdownAwaiter {
	starters := make([]*Waiter, 0, slots)
	for i := uint(0); i < slots; i++ {
		starters = append(starters, NewWaiter())
	}
	msa := &MultiShutdownAwaiter{
		slots:     slots,
		starters:  starters,
		doneGroup: sync.WaitGroup{},
	}
	msa.doneGroup.Add(int(slots))
	return msa
}

func (msa *MultiShutdownAwaiter) SignalShutdown() {
	for i := uint(0); i < msa.slots; i++ {
		msa.starters[i].Signal()
	}
}

func (msa *MultiShutdownAwaiter) AwaitShutdown(slot uint) error {
	return msa.starters[slot].Await()
}

func (msa *MultiShutdownAwaiter) AwaitShutdownChan(slot uint) <-chan bool {
	return msa.starters[slot].done
}

func (msa *MultiShutdownAwaiter) SignalDone() {
	msa.doneGroup.Done()
}

func (msa *MultiShutdownAwaiter) AwaitDone() {
	msa.doneGroup.Wait()
}
