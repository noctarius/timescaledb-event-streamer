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

import "github.com/noctarius/timescaledb-event-streamer/internal/functional"

type UnboundedChannel[T any] struct {
	in  chan<- T
	out <-chan T
}

func MakeUnboundedChannel[T any](
	initialCapacity int,
) *UnboundedChannel[T] {

	in := make(chan T)
	out := make(chan T)

	go func() {
		inQueue := make([]T, 0, initialCapacity)
		outChannel := func() chan T {
			if len(inQueue) == 0 {
				return nil
			}
			return out
		}
		currentValue := func() T {
			if len(inQueue) == 0 {
				return functional.Zero[T]()
			}
			return inQueue[0]
		}
		for len(inQueue) > 0 || in != nil {
			select {
			case v, ok := <-in:
				if !ok {
					in = nil
				} else {
					inQueue = append(inQueue, v)
				}
			case outChannel() <- currentValue():
				inQueue = inQueue[1:]
			}
		}
		close(out)
	}()
	return &UnboundedChannel[T]{
		in:  in,
		out: out,
	}
}

func (uc *UnboundedChannel[T]) Send(
	item T,
) {

	uc.in <- item
}

func (uc *UnboundedChannel[T]) Receive() T {
	return <-uc.out
}

func (uc *UnboundedChannel[T]) ReceiveChannel() <-chan T {
	return uc.out
}

func (uc *UnboundedChannel[T]) Close() {
	close(uc.in)
}
