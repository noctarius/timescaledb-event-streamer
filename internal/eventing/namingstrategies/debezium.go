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

package namingstrategies

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namingstrategy"
)

func init() {
	namingstrategy.RegisterNamingStrategy(config.Debezium,
		func(_ *config.Config) (namingstrategy.NamingStrategy, error) {
			return &DebeziumNamingStrategy{}, nil
		},
	)
}

type DebeziumNamingStrategy struct {
}

func (d *DebeziumNamingStrategy) EventTopicName(topicPrefix string, hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.%s.%s", topicPrefix, hypertable.SchemaName(), hypertable.TableName())
}

func (d *DebeziumNamingStrategy) SchemaTopicName(topicPrefix string, hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.%s.%s", topicPrefix, hypertable.SchemaName(), hypertable.TableName())
}

func (d *DebeziumNamingStrategy) MessageTopicName(topicPrefix string) string {
	return fmt.Sprintf("%s.message", topicPrefix)
}
