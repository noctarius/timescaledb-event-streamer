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

package eventemitting

import (
	"encoding/base64"
	"encoding/binary"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"time"
)

type EventEmitter struct {
	replicationContext context.ReplicationContext
	schemaManager      context.SchemaManager
	stateManager       context.StateManager
	filter             eventfiltering.EventFilter
	sink               sink.Sink
	sinkContext        *sinkContextImpl
	backOff            backoff.BackOff
	logger             *logging.Logger
}

func NewEventEmitter(
	replicationContext context.ReplicationContext, sink sink.Sink, filter eventfiltering.EventFilter) (*EventEmitter, error) {

	logger, err := logging.NewLogger("EventEmitter")
	if err != nil {
		return nil, err
	}

	return &EventEmitter{
		replicationContext: replicationContext,
		schemaManager:      replicationContext.SchemaManager(),
		stateManager:       replicationContext.StateManager(),
		filter:             filter,
		sink:               sink,
		logger:             logger,
		sinkContext:        newSinkContextImpl(),
		backOff:            backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 8),
	}, nil
}

func (ee *EventEmitter) Start() error {
	if encodedSinkContextState, present := ee.stateManager.EncodedState("SinkContextState"); present {
		return ee.sinkContext.UnmarshalBinary(encodedSinkContextState)
	}
	if err := ee.stateManager.StateEncoder(
		"SinkContextState", statestorage.StateEncoderFunc(ee.sinkContext.MarshalBinary),
	); err != nil {
		return errors.Wrap(err, 0)
	}
	return ee.sink.Start()
}

func (ee *EventEmitter) Stop() error {
	return ee.sink.Stop()
}

func (ee *EventEmitter) NewEventHandler() eventhandlers.BaseReplicationEventHandler {
	return &eventEmitterEventHandler{
		eventEmitter: ee,
	}
}

func (ee *EventEmitter) envelopeSchema(hypertable *systemcatalog.Hypertable) schemamodel.Struct {
	schemaTopicName := ee.schemaManager.HypertableEnvelopeSchemaName(hypertable)
	return ee.schemaManager.GetSchemaOrCreate(schemaTopicName, func() schemamodel.Struct {
		return schema.EnvelopeSchema(ee.schemaManager, hypertable)
	})
}

func (ee *EventEmitter) envelopeMessageSchema() schemamodel.Struct {
	schemaTopicName := ee.schemaManager.MessageEnvelopeSchemaName()
	return ee.schemaManager.GetSchemaOrCreate(schemaTopicName, func() schemamodel.Struct {
		return schema.EnvelopeMessageSchema(ee.schemaManager, ee.schemaManager)
	})
}

func (ee *EventEmitter) keySchema(hypertable *systemcatalog.Hypertable) schemamodel.Struct {
	schemaTopicName := ee.schemaManager.HypertableKeySchemaName(hypertable)
	return ee.schemaManager.GetSchemaOrCreate(schemaTopicName, func() schemamodel.Struct {
		return schema.KeySchema(hypertable, ee.schemaManager)
	})
}

func (ee *EventEmitter) emit(xld pgtypes.XLogData, eventTopicName string, key, envelope schemamodel.Struct) error {
	// Retryable operation
	operation := func() error {
		ee.logger.Tracef("Publishing event: %+v", envelope)
		return ee.sink.Emit(ee.sinkContext, xld.ServerTime, eventTopicName, key, envelope)
	}

	// Run with backoff (it'll automatically reset before starting)
	if err := backoff.Retry(operation, ee.backOff); err != nil {
		return err
	}
	return ee.replicationContext.AcknowledgeProcessed(xld, nil)
}

type eventEmitterEventHandler struct {
	eventEmitter *EventEmitter
}

func (e *eventEmitterEventHandler) OnReadEvent(lsn pgtypes.LSN, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, newValues map[string]any) error {

	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	xld := pgtypes.XLogData{
		XLogData: pglogrepl.XLogData{
			WALStart:     pglogrepl.LSN(lsn),
			WALData:      []byte{},
			ServerWALEnd: pglogrepl.LSN(lsn),
			ServerTime:   time.Now(),
		},
	}

	return e.emit0(xld, true, hypertable,
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.ReadEvent(cnValues, source)
		},
		func() (schemamodel.Struct, error) {
			return e.hypertableEventKey(hypertable, newValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnInsertEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, newValues map[string]any) error {

	cnValues, err := e.convertValues(hypertable, newValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable,
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.CreateEvent(cnValues, source)
		},
		func() (schemamodel.Struct, error) {
			return e.hypertableEventKey(hypertable, newValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnUpdateEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable,
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
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.UpdateEvent(coValues, cnValues, source)
		},
		func() (schemamodel.Struct, error) {
			return e.hypertableEventKey(hypertable, newValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnDeleteEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable,
	_ *systemcatalog.Chunk, oldValues map[string]any, tombstone bool) error {

	coValues, err := e.convertValues(hypertable, oldValues)
	if err != nil {
		return err
	}

	return e.emit(xld, hypertable,
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.DeleteEvent(coValues, source, tombstone)
		},
		func() (schemamodel.Struct, error) {
			return e.hypertableEventKey(hypertable, oldValues)
		},
	)
}

func (e *eventEmitterEventHandler) OnTruncateEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable) error {
	return e.emit(xld, hypertable,
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.TruncateEvent(source)
		},
		func() (schemamodel.Struct, error) {
			return nil, nil
		},
	)
}

func (e *eventEmitterEventHandler) OnMessageEvent(xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage) error {
	return e.emitMessageEvent(xld, msg, func(source schemamodel.Struct) schemamodel.Struct {
		content := base64.StdEncoding.EncodeToString(msg.Content)
		return schema.MessageEvent(msg.Prefix, content, source)
	})
}

func (e *eventEmitterEventHandler) OnChunkCompressedEvent(
	xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return e.emit(xld, hypertable,
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.CompressionEvent(source)
		},
		func() (schemamodel.Struct, error) {
			return e.timescaleEventKey(hypertable)
		},
	)
}

func (e *eventEmitterEventHandler) OnChunkDecompressedEvent(
	xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return e.emit(xld, hypertable,
		func(source schemamodel.Struct) schemamodel.Struct {
			return schema.DecompressionEvent(source)
		},
		func() (schemamodel.Struct, error) {
			return e.timescaleEventKey(hypertable)
		},
	)
}

func (e *eventEmitterEventHandler) OnRelationEvent(_ pgtypes.XLogData, _ *pgtypes.RelationMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnBeginEvent(_ pgtypes.XLogData, _ *pgtypes.BeginMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnCommitEvent(_ pgtypes.XLogData, _ *pgtypes.CommitMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnTypeEvent(_ pgtypes.XLogData, _ *pgtypes.TypeMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnOriginEvent(_ pgtypes.XLogData, _ *pgtypes.OriginMessage) error {
	return nil
}

func (e *eventEmitterEventHandler) OnTransactionFinishedEvent(xld pgtypes.XLogData, msg *pgtypes.CommitMessage) error {
	e.eventEmitter.logger.Debugf(
		"Transaction xid=%d (LSN: %s) marked as processed", xld.Xid, msg.TransactionEndLSN.String(),
	)
	transactionEndLSN := pgtypes.LSN(msg.TransactionEndLSN)
	return e.eventEmitter.replicationContext.AcknowledgeProcessed(xld, &transactionEndLSN)
}

func (e *eventEmitterEventHandler) emit(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable,
	eventProvider func(source schemamodel.Struct) schemamodel.Struct, keyProvider func() (schemamodel.Struct, error),
) error {

	return e.emit0(xld, false, hypertable, eventProvider, keyProvider)
}

func (e *eventEmitterEventHandler) emit0(xld pgtypes.XLogData, snapshot bool,
	hypertable *systemcatalog.Hypertable, eventProvider func(source schemamodel.Struct) schemamodel.Struct,
	keyProvider func() (schemamodel.Struct, error)) error {

	envelopeSchema := e.eventEmitter.envelopeSchema(hypertable)
	eventTopicName := e.eventEmitter.schemaManager.EventTopicName(hypertable)

	keyData, err := keyProvider()
	if err != nil {
		return err
	}
	key := schema.Envelope(e.eventEmitter.keySchema(hypertable), keyData)

	event := eventProvider(schema.Source(xld.ServerWALEnd, xld.ServerTime, snapshot,
		hypertable.DatabaseName(), hypertable.SchemaName(), hypertable.TableName(), &xld.Xid))

	value := schema.Envelope(envelopeSchema, event)

	success, err := e.eventEmitter.filter.Evaluate(hypertable, key, value)
	if err != nil {
		return err
	}

	// If unsuccessful we'll discard the event and not send it to the sink
	if !success {
		return e.eventEmitter.replicationContext.AcknowledgeProcessed(xld, nil)
	}

	return e.eventEmitter.emit(xld, eventTopicName, key, value)
}

func (e *eventEmitterEventHandler) emitMessageEvent(xld pgtypes.XLogData,
	msg *pgtypes.LogicalReplicationMessage, eventProvider func(source schemamodel.Struct) schemamodel.Struct) error {

	timestamp := time.Now()
	if msg.IsTransactional() {
		timestamp = xld.ServerTime
	}

	var transactionId *uint32
	if msg.IsTransactional() {
		tid := xld.Xid
		transactionId = &tid
	}

	envelopeSchema := e.eventEmitter.envelopeMessageSchema()
	messageKeySchema := e.eventEmitter.schemaManager.GetSchema(schema.MessageKeySchemaName)

	source := schema.Source(xld.ServerWALEnd, timestamp, false, "", "", "", transactionId)
	payload := eventProvider(source)
	eventTopicName := e.eventEmitter.schemaManager.MessageTopicName()

	key := schema.Envelope(messageKeySchema, schema.MessageKey(msg.Prefix))
	value := schema.Envelope(envelopeSchema, payload)

	return e.eventEmitter.emit(xld, eventTopicName, key, value)
}

func (e *eventEmitterEventHandler) hypertableEventKey(
	hypertable *systemcatalog.Hypertable, values map[string]any) (schemamodel.Struct, error) {

	columns := make([]systemcatalog.Column, 0)
	for _, column := range hypertable.Columns() {
		if !column.IsPrimaryKey() {
			continue
		}
		columns = append(columns, column)
	}
	return e.convertColumnValues(columns, values)
}

func (e *eventEmitterEventHandler) timescaleEventKey(hypertable *systemcatalog.Hypertable) (schemamodel.Struct, error) {
	return schema.TimescaleKey(hypertable.SchemaName(), hypertable.TableName()), nil
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

	typeManager := e.eventEmitter.replicationContext.TypeManager()
	result := make(map[string]any)
	for _, column := range columns {
		if v, present := values[column.Name()]; present {
			converter, err := typeManager.Converter(column.DataType())
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

type sinkContextImpl struct {
	attributes          map[string]string
	transientAttributes map[string]string
}

func newSinkContextImpl() *sinkContextImpl {
	return &sinkContextImpl{
		attributes:          make(map[string]string),
		transientAttributes: make(map[string]string),
	}
}

func (s *sinkContextImpl) UnmarshalBinary(data []byte) error {
	offset := uint32(0)
	numOfItems := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	for i := uint32(0); i < numOfItems; i++ {
		keyLength := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		valueLength := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		key := string(data[offset : offset+keyLength])
		offset += keyLength

		value := string(data[offset : offset+valueLength])
		offset += valueLength

		s.SetAttribute(key, value)
	}
	return nil
}

func (s *sinkContextImpl) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0)

	numOfItems := uint32(len(s.attributes))
	data = binary.BigEndian.AppendUint32(data, numOfItems)

	for key, value := range s.attributes {
		keyBytes := []byte(key)
		valueBytes := []byte(value)

		data = binary.BigEndian.AppendUint32(data, uint32(len(keyBytes)))
		data = binary.BigEndian.AppendUint32(data, uint32(len(valueBytes)))

		data = append(data, keyBytes...)
		data = append(data, valueBytes...)
	}
	return data, nil
}

func (s *sinkContextImpl) SetTransientAttribute(key string, value string) {
	s.transientAttributes[key] = value
}

func (s *sinkContextImpl) TransientAttribute(key string) (value string, present bool) {
	value, present = s.transientAttributes[key]
	return
}

func (s *sinkContextImpl) SetAttribute(key string, value string) {
	s.attributes[key] = value
}

func (s *sinkContextImpl) Attribute(key string) (value string, present bool) {
	value, present = s.attributes[key]
	return
}
