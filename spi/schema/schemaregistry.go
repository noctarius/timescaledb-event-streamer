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
	"sync"
)

type Registry interface {
	RegisterSchema(schemaName string, schema Struct)
	GetSchema(schemaName string) Struct
	GetSchemaOrCreate(schemaName string, creator func() Struct) Struct
	HypertableEnvelopeSchemaName(hypertable TableAlike) string
	HypertableKeySchemaName(hypertable TableAlike) string
	MessageEnvelopeSchemaName() string
}

type registry struct {
	topicNameGenerator NameGenerator
	schemaRegistry     map[string]Struct
	mutex              sync.Mutex
}

func NewRegistry(topicNameGenerator NameGenerator) Registry {
	r := &registry{
		topicNameGenerator: topicNameGenerator,
		schemaRegistry:     make(map[string]Struct),
		mutex:              sync.Mutex{},
	}
	initializeSourceSchemas(r)
	return r
}

func (r *registry) RegisterSchema(schemaName string, schema Struct) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.schemaRegistry[schemaName] = schema
}

func (r *registry) GetSchema(schemaName string) Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.schemaRegistry[schemaName]
}

func (r *registry) GetSchemaOrCreate(schemaName string, creator func() Struct) Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if schema, ok := r.schemaRegistry[schemaName]; ok {
		return schema
	}
	schema := creator()
	r.schemaRegistry[schemaName] = schema
	return schema
}

func (r *registry) HypertableEnvelopeSchemaName(hypertable TableAlike) string {
	return fmt.Sprintf("%s.Envelope", r.topicNameGenerator.SchemaTopicName(hypertable))
}

func (r *registry) HypertableKeySchemaName(hypertable TableAlike) string {
	return fmt.Sprintf("%s.Key", r.topicNameGenerator.SchemaTopicName(hypertable))
}

func (r *registry) MessageEnvelopeSchemaName() string {
	return fmt.Sprintf("%s.Envelope", r.topicNameGenerator.MessageTopicName())
}

func initializeSourceSchemas(registry Registry) {
	registry.RegisterSchema(MessageValueSchemaName, MessageValueSchema(registry))
	registry.RegisterSchema(MessageKeySchemaName, MessageKeySchema())
}
