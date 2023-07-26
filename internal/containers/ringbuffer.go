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
	"runtime"
)

// Channel is based on https://github.com/jtarchie/ringbuffer, but
// extended with the option to skip waiting on an empty output channel
type Channel[T any] struct {
	input  chan T
	output chan T
}

func NewChannel[T any](
	size int,
) *Channel[T] {

	b := &Channel[T]{
		input:  make(chan T, 1),
		output: make(chan T, size),
	}
	go b.run()

	return b
}

func (c *Channel[T]) run() {
	for v := range c.input {
	retry:
		select {
		case c.output <- v:
		default:
			runtime.Gosched()
			goto retry
		}
	}

	close(c.output)
}

func (c *Channel[T]) Send(
	value T,
) {

	c.input <- value
}

func (c *Channel[T]) Receive() T {
	return <-c.output
}

func (c *Channel[T]) ReceiveChannel() <-chan T {
	return c.output
}

func (c *Channel[T]) Close() {
	close(c.input)
}
