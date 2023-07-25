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
	"sync/atomic"
)

type Queue[E any] struct {
	queueChan chan E
	locked    atomic.Bool
}

func NewQueue[E any](maxSize int) *Queue[E] {
	return &Queue[E]{
		queueChan: make(chan E, maxSize),
		locked:    atomic.Bool{},
	}
}

func (rq *Queue[E]) Push(
	v E,
) bool {

	if rq.locked.Load() {
		return false
	}

	rq.queueChan <- v
	return true
}

func (rq *Queue[E]) Pop() E {
	select {
	case v := <-rq.queueChan:
		return v
	default:
		return zero[E]()
	}
}

func (rq *Queue[E]) Close() {
	close(rq.queueChan)
}

func (rq *Queue[E]) Lock() {
	rq.locked.CompareAndSwap(false, true)
}

func zero[T any]() (t T) {
	return
}
