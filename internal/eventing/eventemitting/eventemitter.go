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
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/internal/eventing/eventfiltering"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/stream"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/samber/lo"
	"time"
)

type EventEmitter struct {
	replicationContext context.ReplicationContext
	filter             eventfiltering.EventFilter
	typeManager        pgtypes.TypeManager
	streamManager      stream.Manager
	backOff            backoff.BackOff
	logger             *logging.Logger
}

func NewEventEmitter(
	replicationContext context.ReplicationContext, streamManager stream.Manager,
	typeManager pgtypes.TypeManager, filter eventfiltering.EventFilter,
) (*EventEmitter, error) {

	logger, err := logging.NewLogger("EventEmitter")
	if err != nil {
		return nil, err
	}

	return &EventEmitter{
		replicationContext: replicationContext,
		typeManager:        typeManager,
		streamManager:      streamManager,
		filter:             filter,
		logger:             logger,
		backOff:            backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 8),
	}, nil
}

func (ee *EventEmitter) Start() error {
	return ee.streamManager.Start()
}

func (ee *EventEmitter) Stop() error {
	return ee.streamManager.Stop()
}

func (ee *EventEmitter) Stop1() error {
	return ee.streamManager.Stop()
}

func (ee *EventEmitter) NewEventHandler() eventhandlers.BaseReplicationEventHandler {
	return &eventEmitterEventHandler{
		eventEmitter: ee,
		typeManager:  ee.typeManager,
	}
}

func (ee *EventEmitter) emit(xld pgtypes.XLogData, stream stream.Stream, key, value schema.Struct) error {
	// Retryable operation
	operation := func() error {
		ee.logger.Tracef("Publishing event: %+v", value)
		return stream.Emit(key, value)
	}

	// Run with backoff (it'll automatically reset before starting)
	if err := backoff.Retry(operation, ee.backOff); err != nil {
		return err
	}
	return ee.replicationContext.AcknowledgeProcessed(xld, nil)
}

type eventEmitterEventHandler struct {
	eventEmitter *EventEmitter
	typeManager  pgtypes.TypeManager
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
		func(stream stream.Stream) (schema.Struct, error) {
			return stream.Key(newValues)
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.ReadEvent(cnValues, source), nil
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
		func(stream stream.Stream) (schema.Struct, error) {
			return stream.Key(newValues)
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.CreateEvent(cnValues, source), nil
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
		func(stream stream.Stream) (schema.Struct, error) {
			return stream.Key(newValues)
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.UpdateEvent(coValues, cnValues, source), nil
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
		func(stream stream.Stream) (schema.Struct, error) {
			return stream.Key(oldValues)
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.DeleteEvent(coValues, source, tombstone), nil
		},
	)
}

func (e *eventEmitterEventHandler) OnTruncateEvent(xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable) error {
	return e.emit(xld, hypertable,
		func(stream stream.Stream) (schema.Struct, error) {
			return nil, nil
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.TruncateEvent(source), nil
		},
	)
}

func (e *eventEmitterEventHandler) OnMessageEvent(xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage) error {
	return e.emitMessageEvent(xld, msg,
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			content := base64.StdEncoding.EncodeToString(msg.Content)
			return schema.MessageEvent(msg.Prefix, lo.ToPtr(content), source), nil
		},
	)
}

func (e *eventEmitterEventHandler) OnChunkCompressedEvent(
	xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return e.emit(xld, hypertable,
		func(stream stream.Stream) (schema.Struct, error) {
			return e.timescaleEventKey(hypertable)
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.CompressionEvent(source), nil
		},
	)
}

func (e *eventEmitterEventHandler) OnChunkDecompressedEvent(
	xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable, _ *systemcatalog.Chunk) error {

	return e.emit(xld, hypertable,
		func(stream stream.Stream) (schema.Struct, error) {
			return e.timescaleEventKey(hypertable)
		},
		func(source schema.Struct, stream stream.Stream) (schema.Struct, error) {
			return schema.DecompressionEvent(source), nil
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
		"Transaction xid=%d (LSN: %s) marked as processed", xld.Xid, msg.TransactionEndLSN,
	)
	transactionEndLSN := pgtypes.LSN(msg.TransactionEndLSN)
	return e.eventEmitter.replicationContext.AcknowledgeProcessed(xld, &transactionEndLSN)
}

func (e *eventEmitterEventHandler) emit(
	xld pgtypes.XLogData, hypertable *systemcatalog.Hypertable,
	keyFactory func(stream stream.Stream) (schema.Struct, error),
	payloadFactory func(source schema.Struct, stream stream.Stream) (schema.Struct, error),
) error {

	return e.emit0(xld, false, hypertable, keyFactory, payloadFactory)
}

func (e *eventEmitterEventHandler) emit0(
	xld pgtypes.XLogData, snapshot bool, hypertable *systemcatalog.Hypertable,
	keyFactory func(stream stream.Stream) (schema.Struct, error),
	payloadFactory func(source schema.Struct, stream stream.Stream) (schema.Struct, error),
) error {

	selectedStream := e.eventEmitter.streamManager.GetOrCreateStream(hypertable)
	if selectedStream == nil {
		panic(fmt.Sprintf("Stream for hypertable '%s' is nil", hypertable.CanonicalName()))
	}

	keyStruct, err := keyFactory(selectedStream)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	source := schema.Source(
		xld.ServerWALEnd, xld.ServerTime, snapshot, xld.DatabaseName,
		hypertable.SchemaName(), hypertable.TableName(), &xld.Xid,
	)

	payloadStruct, err := payloadFactory(source, selectedStream)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	key := schema.Envelope(selectedStream.KeySchema(), keyStruct)
	value := schema.Envelope(selectedStream.PayloadSchema(), payloadStruct)

	success, err := e.eventEmitter.filter.Evaluate(hypertable, key, value)
	if err != nil {
		return err
	}

	// If unsuccessful we'll discard the event and not send it to the sink
	if !success {
		return e.eventEmitter.replicationContext.AcknowledgeProcessed(xld, nil)
	}

	return e.eventEmitter.emit(xld, selectedStream, key, value)
}

func (e *eventEmitterEventHandler) emitMessageEvent(
	xld pgtypes.XLogData, msg *pgtypes.LogicalReplicationMessage,
	payloadFactory func(source schema.Struct, stream stream.Stream) (schema.Struct, error),
) error {

	timestamp := time.Now()
	if msg.IsTransactional() {
		timestamp = xld.ServerTime
	}

	var transactionId *uint32
	if msg.IsTransactional() {
		tid := xld.Xid
		transactionId = &tid
	}

	// Nil parameter creates a message stream
	selectedStream := e.eventEmitter.streamManager.GetOrCreateStream(nil)
	if selectedStream == nil {
		panic("Stream for logical messages is nil")
	}

	source := schema.Source(
		xld.ServerWALEnd, timestamp, false, xld.DatabaseName, "", "", transactionId,
	)

	keyStruct, err := selectedStream.Key(map[string]any{"prefix": msg.Prefix})
	if err != nil {
		return errors.Wrap(err, 0)
	}

	payloadStruct, err := payloadFactory(source, selectedStream)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	key := schema.Envelope(selectedStream.KeySchema(), keyStruct)
	value := schema.Envelope(selectedStream.PayloadSchema(), payloadStruct)

	return e.eventEmitter.emit(xld, selectedStream, key, value)
}

func (e *eventEmitterEventHandler) timescaleEventKey(hypertable *systemcatalog.Hypertable) (schema.Struct, error) {
	return schema.TimescaleKey(hypertable.SchemaName(), hypertable.TableName()), nil
}

func (e *eventEmitterEventHandler) convertValues(
	hypertable *systemcatalog.Hypertable, values map[string]any) (map[string]any, error) {

	return e.convertColumnValues(hypertable.TableColumns(), values)
}

func (e *eventEmitterEventHandler) convertColumnValues(
	columns []schema.ColumnAlike, values map[string]any) (map[string]any, error) {

	if values == nil {
		return nil, nil
	}

	result := make(map[string]any)
	for _, column := range columns {
		if v, present := values[column.Name()]; present {
			converter, err := e.typeManager.Converter(column.DataType())
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
