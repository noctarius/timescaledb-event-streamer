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

package context

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/namingstrategy"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReplicationContext_EventTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)

	debeziumNamingStrategy, err := namingstrategy.NewNamingStrategy("debezium", &config.Config{})
	if err != nil {
		t.Error(err)
	}

	schemaManager := &schemaManager{
		topicPrefix:    topicPrefix,
		namingStrategy: debeziumNamingStrategy,
	}

	topicName := schemaManager.EventTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)

}
func TestReplicationContext_SchemaTopicName(t *testing.T) {
	topicPrefix := "foobar"
	hypertable := systemcatalog.NewHypertable(1, "test", "schema", "hypertable", "", "", nil, 0, false, nil, nil, pgtypes.DEFAULT)

	debeziumNamingStrategy, err := namingstrategy.NewNamingStrategy("debezium", &config.Config{})
	if err != nil {
		t.Error(err)
	}

	schemaManager := &schemaManager{
		topicPrefix:    topicPrefix,
		namingStrategy: debeziumNamingStrategy,
	}

	topicName := schemaManager.SchemaTopicName(hypertable)
	assert.Equal(t, "foobar.schema.hypertable", topicName)
}
