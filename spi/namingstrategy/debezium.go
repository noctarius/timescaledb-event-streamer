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

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
)

func init() {
	RegisterNamingStrategy(config.Debezium,
		func(_ *config.Config) (NamingStrategy, error) {
			return &debeziumNamingStrategy{}, nil
		},
	)
}

type debeziumNamingStrategy struct {
}

func (d *debeziumNamingStrategy) EventTopicName(topicPrefix string, hypertable schema.TableAlike) string {
	return fmt.Sprintf("%s.%s.%s", topicPrefix, hypertable.SchemaName(), hypertable.TableName())
}

func (d *debeziumNamingStrategy) SchemaTopicName(topicPrefix string, hypertable schema.TableAlike) string {
	return fmt.Sprintf("%s.%s.%s", topicPrefix, hypertable.SchemaName(), hypertable.TableName())
}

func (d *debeziumNamingStrategy) MessageTopicName(topicPrefix string) string {
	return fmt.Sprintf("%s.message", topicPrefix)
}
