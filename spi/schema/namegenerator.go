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
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
)

// NameGenerator wraps a namingstrategy.NamingStrategy to
// simplify its usage with the topic prefix being predefined
type NameGenerator interface {
	// EventTopicName generates an event topic name for the given table
	EventTopicName(
		table TableAlike,
	) string
	// SchemaTopicName generates a schema topic name for the given v
	SchemaTopicName(
		table TableAlike,
	) string
	// MessageTopicName generates a message topic name for a replication message
	MessageTopicName() string
}

func NewNameGenerator(
	topicPrefix string, namingStrategy namingstrategy.NamingStrategy,
) NameGenerator {

	return &nameGenerator{
		namingStrategy: namingStrategy,
		topicPrefix:    topicPrefix,
	}
}

type nameGenerator struct {
	namingStrategy namingstrategy.NamingStrategy
	topicPrefix    string
}

func (n *nameGenerator) EventTopicName(
	table TableAlike,
) string {

	return n.namingStrategy.EventTopicName(n.topicPrefix, table.SchemaName(), table.TableName())
}

func (n *nameGenerator) SchemaTopicName(
	table TableAlike,
) string {

	return n.namingStrategy.SchemaTopicName(n.topicPrefix, table.SchemaName(), table.TableName())
}

func (n *nameGenerator) MessageTopicName() string {
	return n.namingStrategy.MessageTopicName(n.topicPrefix)
}
