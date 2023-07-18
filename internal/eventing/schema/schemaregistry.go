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

package schema

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namegenerator"
	"sync"
)

type Registry struct {
	topicNameGenerator namegenerator.NameGenerator
	schemaRegistry     map[string]schemamodel.Struct
	mutex              sync.Mutex
}

func NewRegistry(topicNameGenerator namegenerator.NameGenerator) schema.Registry {
	r := &Registry{
		topicNameGenerator: topicNameGenerator,
		schemaRegistry:     make(map[string]schemamodel.Struct),
		mutex:              sync.Mutex{},
	}
	initializeSourceSchemas(r)
	return r
}

func (r *Registry) RegisterSchema(schemaName string, schema schemamodel.Struct) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.schemaRegistry[schemaName] = schema
}

func (r *Registry) GetSchema(schemaName string) schemamodel.Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.schemaRegistry[schemaName]
}

func (r *Registry) GetSchemaOrCreate(schemaName string, creator func() schemamodel.Struct) schemamodel.Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if schema, ok := r.schemaRegistry[schemaName]; ok {
		return schema
	}
	schema := creator()
	r.schemaRegistry[schemaName] = schema
	return schema
}

func (r *Registry) HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.Envelope", r.topicNameGenerator.SchemaTopicName(hypertable))
}

func (r *Registry) HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.Key", r.topicNameGenerator.SchemaTopicName(hypertable))
}

func (r *Registry) MessageEnvelopeSchemaName() string {
	return fmt.Sprintf("%s.Envelope", r.topicNameGenerator.MessageTopicName())
}

func initializeSourceSchemas(registry schema.Registry) {
	registry.RegisterSchema(schema.MessageValueSchemaName, schema.MessageValueSchema(registry))
	registry.RegisterSchema(schema.MessageKeySchemaName, schema.MessageKeySchema())
}
