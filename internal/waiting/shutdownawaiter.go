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

package waiting

import "sync"

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

func NewMultiShutdownAwaiter(
	slots uint,
) *MultiShutdownAwaiter {

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

func (msa *MultiShutdownAwaiter) AwaitShutdown(
	slot uint,
) error {

	return msa.starters[slot].Await()
}

func (msa *MultiShutdownAwaiter) AwaitShutdownChan(
	slot uint,
) <-chan bool {

	return msa.starters[slot].done
}

func (msa *MultiShutdownAwaiter) SignalDone() {
	msa.doneGroup.Done()
}

func (msa *MultiShutdownAwaiter) AwaitDone() {
	msa.doneGroup.Wait()
}
