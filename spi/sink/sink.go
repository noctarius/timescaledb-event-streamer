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

package sink

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"time"
)

type Factory = func(config *config.Config) (Sink, error)

type Sink interface {
	Start() error
	Stop() error
	Emit(
		context Context, timestamp time.Time, topicName string, key, envelope schema.Struct,
	) error
}

type SinkFunc func(context Context, timestamp time.Time, topicName string, key, envelope schema.Struct) error

func (sf SinkFunc) Start() error {
	return nil
}

func (sf SinkFunc) Stop() error {
	return nil
}

func (sf SinkFunc) Emit(
	context Context, timestamp time.Time, topicName string, key, envelope schema.Struct,
) error {

	return sf(context, timestamp, topicName, key, envelope)
}
