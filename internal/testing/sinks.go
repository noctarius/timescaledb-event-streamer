package testing

import (
	"encoding/json"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"sync"
	"time"
)

type ForwarderSink struct {
	fn func(timestamp time.Time, topicName string, envelope schema.Struct) error
}

func NewForwarderSink(fn func(timestamp time.Time, topicName string, envelope schema.Struct) error) *ForwarderSink {
	return &ForwarderSink{
		fn: fn,
	}
}

func (t *ForwarderSink) Emit(timestamp time.Time, topicName string, envelope schema.Struct) error {
	return t.fn(timestamp, topicName, envelope)
}

type CollectedEvent struct {
	Timestamp time.Time
	TopicName string
	Envelope  Envelope
}

type EventCollectorSink struct {
	mutex sync.Mutex

	keys   []schema.Struct
	events []CollectedEvent
	filter func(timestamp time.Time, topicName string, envelope Envelope) bool

	preHook  func(sink *EventCollectorSink)
	postHook func(sink *EventCollectorSink)
}

type EventCollectorSinkOption = func(eventCollectorSink *EventCollectorSink)

func WithFilter(filter func(timestamp time.Time, topicName string, envelope Envelope) bool) EventCollectorSinkOption {
	return func(eventCollectorSink *EventCollectorSink) {
		eventCollectorSink.filter = filter
	}
}

func WithPreHook(fn func(sink *EventCollectorSink)) EventCollectorSinkOption {
	return func(eventCollectorSink *EventCollectorSink) {
		eventCollectorSink.preHook = fn
	}
}

func WithPostHook(fn func(sink *EventCollectorSink)) EventCollectorSinkOption {
	return func(eventCollectorSink *EventCollectorSink) {
		eventCollectorSink.postHook = fn
	}
}

func NewEventCollectorSink(options ...EventCollectorSinkOption) *EventCollectorSink {
	eventCollectorSink := &EventCollectorSink{
		keys:   make([]schema.Struct, 0),
		events: make([]CollectedEvent, 0),
		mutex:  sync.Mutex{},
	}
	for _, option := range options {
		option(eventCollectorSink)
	}
	return eventCollectorSink
}

func (t *EventCollectorSink) SystemConfigConfigurator(config *sysconfig.SystemConfig) {
	config.SinkProvider = func(_ *spiconfig.Config) (sink.Sink, error) {
		return t, nil
	}
}

func (t *EventCollectorSink) Events() []CollectedEvent {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.events
}

func (t *EventCollectorSink) NumOfEvents() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return len(t.events)
}

func (t *EventCollectorSink) Emit(_ sink.Context, timestamp time.Time,
	topicName string, key, envelope schema.Struct) error {

	if t.preHook != nil {
		t.preHook(t)
	}
	d, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	var eventEnvelope Envelope
	if err := json.Unmarshal(d, &eventEnvelope); err != nil {
		return err
	}
	if t.filter != nil {
		if !t.filter(timestamp, topicName, eventEnvelope) {
			return nil
		}
	}
	t.mutex.Lock()
	t.keys = append(t.keys, key)
	t.events = append(t.events, CollectedEvent{
		Timestamp: timestamp,
		TopicName: topicName,
		Envelope:  eventEnvelope,
	})
	t.mutex.Unlock()
	if t.postHook != nil {
		t.postHook(t)
	}
	return nil
}
