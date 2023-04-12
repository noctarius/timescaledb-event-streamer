package sink

import (
	"encoding/base64"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/event/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namegenerator"
	"time"
)

type EventEmitter struct {
	schemaRegistry     *schema.Registry
	topicNameGenerator *namegenerator.NameGenerator
	transactionMonitor *transactional.TransactionMonitor
	filter             eventfiltering.Filter
	sink               sink.Sink
}

func NewEventEmitter(schemaRegistry *schema.Registry, topicNameGenerator *namegenerator.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor, sink sink.Sink, filter eventfiltering.Filter) *EventEmitter {

	return &EventEmitter{
		schemaRegistry:     schemaRegistry,
		topicNameGenerator: topicNameGenerator,
		transactionMonitor: transactionMonitor,
		filter:             filter,
		sink:               sink,
	}
}

func (ee *EventEmitter) NewEventHandler() eventhandlers.BaseReplicationEventHandler {
	return &eventEmitterEventHandler{
		eventEmitter: ee,
	}
}

func (ee *EventEmitter) envelopeSchema(hypertable *systemcatalog.Hypertable) schema.Struct {
	schemaTopicName := ee.schemaRegistry.HypertableEnvelopeSchemaName(hypertable)
	return ee.schemaRegistry.GetSchemaOrCreate(schemaTopicName, func() schema.Struct {
		return schema.EnvelopeSchema(ee.schemaRegistry, hypertable, ee.topicNameGenerator)
	})
}

func (ee *EventEmitter) envelopeMessageSchema() schema.Struct {
	schemaTopicName := ee.schemaRegistry.MessageEnvelopeSchemaName()
	return ee.schemaRegistry.GetSchemaOrCreate(schemaTopicName, func() schema.Struct {
		return schema.EnvelopeMessageSchema(ee.schemaRegistry, ee.topicNameGenerator)
	})
}

func (ee *EventEmitter) keySchema(hypertable *systemcatalog.Hypertable) schema.Struct {
	schemaTopicName := ee.schemaRegistry.HypertableKeySchemaName(hypertable)
	return ee.schemaRegistry.GetSchemaOrCreate(schemaTopicName, func() schema.Struct {
		return schema.KeySchema(hypertable, ee.topicNameGenerator)
	})
}

func (ee *EventEmitter) emit(eventTopicName string, timestamp time.Time, key, envelope schema.Struct) error {
	return ee.sink.Emit(timestamp, eventTopicName, key, envelope)
}

type eventEmitterEventHandler struct {
	eventEmitter *EventEmitter
}

func (e *eventEmitterEventHandler) OnReadEvent(lsn pglogrepl.LSN, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, newValues map[string]any) error {

	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	return e.emit0(lsn, time.Now(), true, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.ReadEvent(cnValues, source)
		},
		func() (schema.Struct, error) {
			return e.hypertableEventKey(hypertable, newValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnInsertEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, newValues map[string]any) error {

	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.CreateEvent(cnValues, source)
		},
		func() (schema.Struct, error) {
			return e.hypertableEventKey(hypertable, newValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnUpdateEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, oldValues, newValues map[string]any) error {

	coValues, err := e.convertValues(hypertable, oldValues)
	if err != nil {
		return err
	}
	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.UpdateEvent(coValues, cnValues, source)
		},
		func() (schema.Struct, error) {
			return e.hypertableEventKey(hypertable, newValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnDeleteEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, oldValues map[string]any, tombstone bool) error {

	coValues, err := e.convertValues(hypertable, oldValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.DeleteEvent(coValues, source, tombstone)
		},
		func() (schema.Struct, error) {
			return e.hypertableEventKey(hypertable, oldValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnTruncateEvent(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable) error {
	return e.emit(xld, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.TruncateEvent(source)
		},
		func() (schema.Struct, error) {
			return nil, nil
		},
	)
}

func (e *eventEmitterEventHandler) OnMessageEvent(
	xld pglogrepl.XLogData, msg *pgtypes.LogicalReplicationMessage) error {

	return e.emitMessageEvent(xld, msg, func(source schema.Struct) schema.Struct {
		content := base64.StdEncoding.EncodeToString(msg.Content)
		return schema.MessageEvent(msg.Prefix, content, source)
	})
}

func (e *eventEmitterEventHandler) OnChunkCompressedEvent(
	xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return e.emit(xld, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.CompressionEvent(source)
		},
		func() (schema.Struct, error) {
			return e.timescaleEventKey(hypertable)
		},
	)
}

func (e *eventEmitterEventHandler) OnChunkDecompressedEvent(
	xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return e.emit(xld, hypertable,
		func(source schema.Struct) schema.Struct {
			return schema.DecompressionEvent(source)
		},
		func() (schema.Struct, error) {
			return e.timescaleEventKey(hypertable)
		},
	)
}

func (e *eventEmitterEventHandler) OnRelationEvent(_ pglogrepl.XLogData, _ *pgtypes.RelationMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnBeginEvent(_ pglogrepl.XLogData, _ *pgtypes.BeginMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnCommitEvent(_ pglogrepl.XLogData, _ *pgtypes.CommitMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnTypeEvent(_ pglogrepl.XLogData, _ *pgtypes.TypeMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnOriginEvent(_ pglogrepl.XLogData, _ *pgtypes.OriginMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) emit(xld pglogrepl.XLogData, hypertable *systemcatalog.Hypertable,
	eventProvider func(source schema.Struct) schema.Struct, keyProvider func() (schema.Struct, error)) error {

	return e.emit0(xld.ServerWALEnd, xld.ServerTime, false, hypertable, eventProvider, keyProvider)
}

func (e *eventEmitterEventHandler) emit0(lsn pglogrepl.LSN, timestamp time.Time, snapshot bool,
	hypertable *systemcatalog.Hypertable, eventProvider func(source schema.Struct) schema.Struct,
	keyProvider func() (schema.Struct, error)) error {

	transactionId := e.eventEmitter.transactionMonitor.TransactionId()
	envelopeSchema := e.eventEmitter.envelopeSchema(hypertable)
	eventTopicName := e.eventEmitter.topicNameGenerator.EventTopicName(hypertable)

	keyData, err := keyProvider()
	if err != nil {
		return err
	}
	key := schema.Envelope(e.eventEmitter.keySchema(hypertable), keyData)

	event := eventProvider(schema.Source(lsn, timestamp, snapshot, hypertable.DatabaseName(),
		hypertable.SchemaName(), hypertable.HypertableName(), &transactionId))

	value := schema.Envelope(envelopeSchema, event)

	success, err := e.eventEmitter.filter.Evaluate(hypertable, key, value)
	if err != nil {
		return err
	}

	// If unsuccessful we'll discard the event and not send it to the sink
	if !success {
		return nil
	}

	return e.eventEmitter.emit(eventTopicName, timestamp, key, value)
}

func (e *eventEmitterEventHandler) emitMessageEvent(xld pglogrepl.XLogData,
	msg *pgtypes.LogicalReplicationMessage, eventProvider func(source schema.Struct) schema.Struct) error {

	timestamp := time.Now()
	if msg.IsTransactional() {
		timestamp = xld.ServerTime
	}

	var transactionId *uint32
	if msg.IsTransactional() {
		tid := e.eventEmitter.transactionMonitor.TransactionId()
		transactionId = &tid
	}

	envelopeSchema := e.eventEmitter.envelopeMessageSchema()
	messageKeySchema := e.eventEmitter.schemaRegistry.GetSchema(schema.MessageKeySchemaName)

	source := schema.Source(xld.ServerWALEnd, timestamp, false, "", "", "", transactionId)
	payload := eventProvider(source)
	eventTopicName := e.eventEmitter.topicNameGenerator.MessageTopicName()

	key := schema.Envelope(messageKeySchema, schema.MessageKey(msg.Prefix))
	value := schema.Envelope(envelopeSchema, payload)

	return e.eventEmitter.emit(eventTopicName, timestamp, key, value)
}

func (e *eventEmitterEventHandler) hypertableEventKey(
	hypertable *systemcatalog.Hypertable, values map[string]any) (schema.Struct, error) {

	columns := make([]systemcatalog.Column, 0)
	for _, column := range hypertable.Columns() {
		if !column.IsPrimaryKey() {
			continue
		}
		columns = append(columns, column)
	}
	return e.convertColumnValues(columns, values)
}

func (e *eventEmitterEventHandler) timescaleEventKey(hypertable *systemcatalog.Hypertable) (schema.Struct, error) {
	return schema.TimescaleKey(hypertable.SchemaName(), hypertable.HypertableName()), nil
}

func (e *eventEmitterEventHandler) convertValues(
	hypertable *systemcatalog.Hypertable, values map[string]any) (map[string]any, error) {

	return e.convertColumnValues(hypertable.Columns(), values)
}

func (e *eventEmitterEventHandler) convertColumnValues(
	columns []systemcatalog.Column, values map[string]any) (map[string]any, error) {

	if values == nil {
		return nil, nil
	}

	result := make(map[string]any)
	for _, column := range columns {
		if v, present := values[column.Name()]; present {
			converter, err := systemcatalog.ConverterByOID(column.DataType())
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
