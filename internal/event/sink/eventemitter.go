package sink

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/eventhandler"
	"github.com/noctarius/event-stream-prototype/internal/replication/transactional"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"time"
)

type EventEmitter struct {
	schemaRegistry     *schema.Registry
	topicNameGenerator *topic.NameGenerator
	transactionMonitor *transactional.TransactionMonitor
	sink               Sink
}

func NewEventEmitter(schemaRegistry *schema.Registry, topicNameGenerator *topic.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor, sink Sink) *EventEmitter {

	return &EventEmitter{
		schemaRegistry:     schemaRegistry,
		topicNameGenerator: topicNameGenerator,
		transactionMonitor: transactionMonitor,
		sink:               sink,
	}
}

func (ee *EventEmitter) NewEventHandler() eventhandler.BaseReplicationEventHandler {
	return &eventEmitterEventHandler{
		eventEmitter: ee,
	}
}

func (ee *EventEmitter) envelopeSchema(hypertable *model.Hypertable) schema.Struct {
	schemaTopicName := fmt.Sprintf("%s.Envelope", ee.topicNameGenerator.SchemaTopicName(hypertable))
	return ee.schemaRegistry.GetSchemaOrCreate(schemaTopicName, func() schema.Struct {
		return schema.EnvelopeSchema(ee.schemaRegistry, hypertable, ee.topicNameGenerator)
	})
}

func (ee *EventEmitter) emit(hypertable *model.Hypertable, timestamp time.Time, envelope schema.Struct) error {
	eventTopicName := ee.topicNameGenerator.EventTopicName(hypertable)
	return ee.sink.Emit(timestamp, eventTopicName, envelope)
}

type eventEmitterEventHandler struct {
	eventEmitter *EventEmitter
}

func (e *eventEmitterEventHandler) OnReadEvent(lsn pglogrepl.LSN, hypertable *model.Hypertable,
	_ *model.Chunk, newValues map[string]any) error {

	return e.emit0(lsn, time.Now(), true, hypertable, func(source schema.Struct) schema.Struct {
		return schema.ReadEvent(newValues, source)
	})
}

func (e *eventEmitterEventHandler) OnRelationEvent(_ pglogrepl.XLogData, _ *pglogrepl.RelationMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnBeginEvent(_ pglogrepl.XLogData, _ *pglogrepl.BeginMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnCommitEvent(_ pglogrepl.XLogData, _ *pglogrepl.CommitMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnInsertEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable,
	_ *model.Chunk, newValues map[string]any) error {

	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable, func(source schema.Struct) schema.Struct {
		return schema.CreateEvent(cnValues, source)
	})
}

func (e *eventEmitterEventHandler) OnUpdateEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable,
	_ *model.Chunk, oldValues, newValues map[string]any) error {

	coValues, err := e.convertValues(hypertable, oldValues)
	if err != nil {
		return err
	}
	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable, func(source schema.Struct) schema.Struct {
		return schema.UpdateEvent(coValues, cnValues, source)
	})
}

func (e *eventEmitterEventHandler) OnDeleteEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable,
	_ *model.Chunk, oldValues map[string]any) error {

	coValues, err := e.convertValues(hypertable, oldValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable, func(source schema.Struct) schema.Struct {
		return schema.DeleteEvent(coValues, source)
	})
}

func (e *eventEmitterEventHandler) OnTruncateEvent(xld pglogrepl.XLogData, hypertable *model.Hypertable) error {
	return e.emit(xld, hypertable, func(source schema.Struct) schema.Struct {
		return schema.TruncateEvent(source)
	})
}

func (e *eventEmitterEventHandler) OnTypeEvent(_ pglogrepl.XLogData, _ *pglogrepl.TypeMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnOriginEvent(_ pglogrepl.XLogData, _ *pglogrepl.OriginMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) emit(xld pglogrepl.XLogData, hypertable *model.Hypertable,
	eventProvider func(source schema.Struct) schema.Struct) error {

	return e.emit0(xld.ServerWALEnd, xld.ServerTime, false, hypertable, eventProvider)
}

func (e *eventEmitterEventHandler) emit0(lsn pglogrepl.LSN, timestamp time.Time, snapshot bool,
	hypertable *model.Hypertable, eventProvider func(source schema.Struct) schema.Struct) error {

	envelopeSchema := e.eventEmitter.envelopeSchema(hypertable)
	source := schema.Source(lsn, timestamp, snapshot, hypertable, e.eventEmitter.transactionMonitor.TransactionId())
	payload := eventProvider(source)
	return e.eventEmitter.emit(hypertable, timestamp, schema.Envelope(envelopeSchema, payload))
}

func (e *eventEmitterEventHandler) convertValues(hypertable *model.Hypertable,
	values map[string]any) (map[string]any, error) {

	if values == nil {
		return nil, nil
	}

	result := make(map[string]any)
	for _, column := range hypertable.Columns() {
		if v, present := values[column.Name()]; present {
			converter, err := model.ConverterByOID(column.DataType())
			if err != nil {
				return nil, err
			}
			if converter != nil {
				v, err = converter(column.DataType(), v)
				if err != nil {
					return nil, err
				}
			}
			result[column.Name()] = v
		}
	}
	return result, nil
}

func (e *eventEmitterEventHandler) OnChunkCompressedEvent(
	xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk) error {

	return e.emit(xld, hypertable, func(source schema.Struct) schema.Struct {
		return schema.CompressionEvent(source)
	})
}

func (e *eventEmitterEventHandler) OnChunkDecompressedEvent(
	xld pglogrepl.XLogData, hypertable *model.Hypertable, chunk *model.Chunk) error {

	return e.emit(xld, hypertable, func(source schema.Struct) schema.Struct {
		return schema.DecompressionEvent(source)
	})
}
