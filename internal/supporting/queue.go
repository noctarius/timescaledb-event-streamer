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
	"container/list"
	"sync"
)

type Queue[E any] struct {
	queue  *list.List
	mutex  sync.Mutex
	locked bool
}

func NewQueue[E any]() *Queue[E] {
	return &Queue[E]{
		queue: list.New(),
		mutex: sync.Mutex{},
	}
}

func (rq *Queue[E]) Push(
	fn E,
) bool {

	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	if rq.locked {
		return false
	}

	rq.queue.PushBack(fn)
	return true
}

func (rq *Queue[E]) Pop() E {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	e := rq.queue.Front()
	if e == nil {
		return *new(E)
	}
	rq.queue.Remove(e)
	return e.Value.(E)
}

func (rq *Queue[E]) Lock() {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	rq.locked = true
}
