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

package namingstrategy

import "github.com/noctarius/timescaledb-event-streamer/spi/config"

type Factory func(config *config.Config) (NamingStrategy, error)

// NamingStrategy represents a strategy to generate
// topic names for event topics, schema topics, as
// well as message topics
type NamingStrategy interface {
	// EventTopicName generates a event topic name for the given schema and table name
	EventTopicName(
		topicPrefix string, schemaName, tableName string,
	) string
	// SchemaTopicName generates a schema topic name for the given schema and table name
	SchemaTopicName(
		topicPrefix string, schemaName, tableName string,
	) string
	// MessageTopicName generates a message topic name for a replication message
	MessageTopicName(
		topicPrefix string,
	) string
}
