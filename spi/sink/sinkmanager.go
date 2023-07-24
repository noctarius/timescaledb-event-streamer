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
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"time"
)

const (
	sinkContextStateName = "SinkContextState"
)

type Manager interface {
	Start() error
	Stop() error
	Emit(
		timestamp time.Time, topicName string, key, envelope schema.Struct,
	) error
}

type sinkManager struct {
	stateStorageManager statestorage.Manager
	sinkContext         *sinkContext
	sink                Sink
}

func NewSinkManager(
	stateStorageManager statestorage.Manager, sink Sink,
) Manager {

	return &sinkManager{
		stateStorageManager: stateStorageManager,
		sinkContext:         newSinkContext(),
		sink:                sink,
	}
}

func (sm *sinkManager) Start() error {
	if encodedSinkContextState, present := sm.stateStorageManager.EncodedState(sinkContextStateName); present {
		return sm.sinkContext.UnmarshalBinary(encodedSinkContextState)
	}
	return sm.sink.Start()
}

func (sm *sinkManager) Stop() error {
	if err := sm.stateStorageManager.StateEncoder(
		sinkContextStateName, statestorage.StateEncoderFunc(sm.sinkContext.MarshalBinary),
	); err != nil {
		return errors.Wrap(err, 0)
	}
	return sm.sink.Stop()
}

func (sm *sinkManager) Emit(
	timestamp time.Time, topicName string, key, envelope schema.Struct,
) error {

	return sm.sink.Emit(sm.sinkContext, timestamp, topicName, key, envelope)
}
