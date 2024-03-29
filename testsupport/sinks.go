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

package testsupport

import (
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"sync"
	"time"
)

type ForwarderSink struct {
	fn func(timestamp time.Time, topicName string, envelope schema.Struct) error
}

func NewForwarderSink(
	fn func(timestamp time.Time, topicName string, envelope schema.Struct) error,
) *ForwarderSink {

	return &ForwarderSink{
		fn: fn,
	}
}

func (t *ForwarderSink) Emit(
	timestamp time.Time, topicName string, envelope schema.Struct,
) error {

	return t.fn(timestamp, topicName, envelope)
}

type CollectedEvent struct {
	Timestamp time.Time
	TopicName string
	Envelope  Envelope
}

type EventCollectorSink struct {
	mutex   sync.Mutex
	encoder *encoding.JsonEncoder
	decoder *encoding.JsonDecoder

	keys   []schema.Struct
	events []CollectedEvent
	filter func(timestamp time.Time, topicName string, envelope Envelope) bool

	preHook  func(sink *EventCollectorSink)
	postHook func(sink *EventCollectorSink, envelope Envelope)
}

func (t *EventCollectorSink) Start() error {
	return nil
}

func (t *EventCollectorSink) Stop() error {
	return nil
}

type EventCollectorSinkOption = func(eventCollectorSink *EventCollectorSink)

func WithFilter(
	filter func(timestamp time.Time, topicName string, envelope Envelope) bool,
) EventCollectorSinkOption {

	return func(eventCollectorSink *EventCollectorSink) {
		eventCollectorSink.filter = filter
	}
}

func WithPreHook(
	fn func(sink *EventCollectorSink),
) EventCollectorSinkOption {

	return func(eventCollectorSink *EventCollectorSink) {
		eventCollectorSink.preHook = fn
	}
}

func WithPostHook(
	fn func(sink *EventCollectorSink, envelope Envelope),
) EventCollectorSinkOption {

	return func(eventCollectorSink *EventCollectorSink) {
		eventCollectorSink.postHook = fn
	}
}

func NewEventCollectorSink(
	options ...EventCollectorSinkOption,
) *EventCollectorSink {

	eventCollectorSink := &EventCollectorSink{
		keys:    make([]schema.Struct, 0),
		events:  make([]CollectedEvent, 0),
		mutex:   sync.Mutex{},
		encoder: encoding.NewJsonEncoder(true),
		decoder: encoding.NewJsonDecoder(true),
	}
	for _, option := range options {
		option(eventCollectorSink)
	}
	return eventCollectorSink
}

func (t *EventCollectorSink) SystemConfigConfigurator(
	config *sysconfig.SystemConfig,
) {

	config.SinkManagerProvider = func(
		stateStorageManager statestorage.Manager, s sink.Sink,
	) sink.Manager {

		return sinkimpl.NewSinkManager(stateStorageManager, t)
	}
}

func (t *EventCollectorSink) Events() []CollectedEvent {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.events
}

func (t *EventCollectorSink) Clear() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.keys = make([]schema.Struct, 0)
	t.events = make([]CollectedEvent, 0)
}

func (t *EventCollectorSink) NumOfEvents() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return len(t.events)
}

func (t *EventCollectorSink) Emit(
	_ sink.Context, timestamp time.Time, topicName string, key, envelope schema.Struct,
) error {

	if t.preHook != nil {
		t.preHook(t)
	}
	d, err := t.encoder.Marshal(envelope)
	if err != nil {
		return err
	}
	var eventEnvelope Envelope
	if err := t.decoder.Unmarshal(d, &eventEnvelope); err != nil {
		return err
	}
	if t.filter != nil {
		if !t.filter(timestamp, topicName, eventEnvelope) {
			return nil
		}
	}
	eventEnvelope.Raw = envelope
	t.mutex.Lock()
	t.keys = append(t.keys, key)
	t.events = append(t.events, CollectedEvent{
		Timestamp: timestamp,
		TopicName: topicName,
		Envelope:  eventEnvelope,
	})
	t.mutex.Unlock()
	if t.postHook != nil {
		t.postHook(t, eventEnvelope)
	}
	return nil
}
