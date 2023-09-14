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

package stream

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

type Stream interface {
	KeySchema() schema.Struct
	Key(
		values map[string]any,
	) (schema.Struct, error)
	PayloadSchema() schema.Struct
	Emit(
		key, envelope schema.Struct,
	) error
}

type tableStreamImpl struct {
	sinkManager     sink.Manager
	typeManager     pgtypes.TypeManager
	tableDefinition schema.TableAlike
	tableKeyColumns []schema.ColumnAlike

	topicName      string
	keySchema      schema.Struct
	envelopeSchema schema.Struct
}

func NewTableStream(
	nameGenerator schema.NameGenerator, typeManager pgtypes.TypeManager,
	sinkManager sink.Manager, tableDefinition schema.TableAlike,
) Stream {

	return &tableStreamImpl{
		sinkManager:     sinkManager,
		typeManager:     typeManager,
		tableDefinition: tableDefinition,
		tableKeyColumns: tableDefinition.KeyIndexColumns(),

		topicName:      nameGenerator.EventTopicName(tableDefinition),
		keySchema:      schema.KeySchema(nameGenerator, tableDefinition),
		envelopeSchema: schema.EnvelopeSchema(nameGenerator, tableDefinition),
	}
}

func (s *tableStreamImpl) KeySchema() schema.Struct {
	return s.keySchema
}

func (s *tableStreamImpl) PayloadSchema() schema.Struct {
	return s.envelopeSchema
}

func (s *tableStreamImpl) Key(
	values map[string]any,
) (schema.Struct, error) {

	result := make(map[string]any)
	for _, column := range s.tableKeyColumns {
		if v, present := values[column.Name()]; present {
			if v != nil {
				converter, err := s.typeManager.ResolveTypeConverter(column.DataType())
				if err != nil {
					return nil, err
				}
				if converter != nil {
					v, err = converter(column.DataType(), v)
					if err != nil {
						return nil, err
					}
				}
			}
			result[column.Name()] = v
		}
	}
	return result, nil
}

func (s *tableStreamImpl) Emit(
	key, envelope schema.Struct,
) error {

	return s.sinkManager.Emit(time.Now(), s.topicName, key, envelope)
}

type messageStreamImpl struct {
	sinkManager sink.Manager

	topicName      string
	keySchema      schema.Struct
	envelopeSchema schema.Struct
}

func NewMessageStream(
	nameGenerator schema.NameGenerator, sinkManager sink.Manager,
) Stream {

	return &messageStreamImpl{
		sinkManager: sinkManager,

		topicName:      nameGenerator.MessageTopicName(),
		keySchema:      schema.MessageKeySchema(),
		envelopeSchema: schema.EnvelopeMessageSchema(nameGenerator),
	}
}

func (m *messageStreamImpl) KeySchema() schema.Struct {
	return m.keySchema
}

func (m *messageStreamImpl) PayloadSchema() schema.Struct {
	return m.envelopeSchema
}

func (m *messageStreamImpl) Key(
	values map[string]any,
) (schema.Struct, error) {

	prefix, present := values["prefix"]
	if !present {
		return nil, errors.Errorf("prefix not set for message event")
	}
	return schema.Struct{
		schema.FieldNamePrefix: prefix.(string),
	}, nil
}

func (m *messageStreamImpl) Emit(
	key, envelope schema.Struct,
) error {

	return m.sinkManager.Emit(time.Now(), m.topicName, key, envelope)
}
