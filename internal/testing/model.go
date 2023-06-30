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

package testing

import "github.com/noctarius/timescaledb-event-streamer/spi/schema"

type Source struct {
	Connector string `json:"connector"`
	DB        string `json:"db"`
	LSN       string `json:"lsn"`
	TxId      uint32 `json:"txId"`
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Snapshot  bool   `json:"snapshot"`
	Table     string `json:"table"`
	TsMs      uint64 `json:"ts_ms"`
	Version   string `json:"version"`
}

type Payload struct {
	Before map[string]any            `json:"before"`
	After  map[string]any            `json:"after"`
	Op     schema.Operation          `json:"op"`
	TsdbOp schema.TimescaleOperation `json:"tsdb_op"`
	Source Source                    `json:"source"`
	TsMs   uint64                    `json:"ts_ms"`
}

type Field struct {
	Name     string  `json:"name"`
	Field    string  `json:"field"`
	Optional bool    `json:"optional"`
	Type     string  `json:"type"`
	Fields   []Field `json:"fields"`
	Default  any     `json:"default"`
}

type Schema struct {
	Fields   []Field `json:"fields"`
	Name     string  `json:"name"`
	Optional bool    `json:"optional"`
	Type     string  `json:"type"`
}

type Envelope struct {
	Payload Payload `json:"payload"`
	Schema  Schema  `json:"schema"`
}
