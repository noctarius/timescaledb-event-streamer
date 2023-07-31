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

package pgtypes

import (
	"encoding/binary"
	"fmt"
	"github.com/jackc/pglogrepl"
	"strings"
)

const (
	MessageTypeLogicalDecodingMessage pglogrepl.MessageType = 'M'
)

// LogicalReplicationMessage is a logical replication message.
type LogicalReplicationMessage struct {
	baseMessage
	// Flags is either 0 (non-transactional) or 1 (transactional)
	Flags uint8
	// Xid is the transaction id (if transactional logical replication message)
	Xid *uint32
	// LSN is the LSN of the logical replication message
	LSN pglogrepl.LSN
	// Prefix is the prefix of the logical replication message
	Prefix string
	// Content is the content of the logical replication message
	Content []byte
}

func (m *LogicalReplicationMessage) Decode(
	src []byte,
) (err error) {

	var low, used int
	m.Flags = src[0]
	low += 1
	m.LSN, used = m.decodeLSN(src[low:])
	low += used
	m.Prefix, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("LogicalReplicationMessage", "Prefix")
	}
	low += used
	contentLength := binary.BigEndian.Uint32(src[low:])
	low += 4
	m.Content = src[low : low+int(contentLength)]

	m.SetType(MessageTypeLogicalDecodingMessage)

	return nil
}

func (m *LogicalReplicationMessage) IsTransactional() bool {
	return m.Flags == 1
}

func (m *LogicalReplicationMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("prefix:%s ", m.Prefix))
	builder.WriteString(fmt.Sprintf("content:%x ", m.Content))
	builder.WriteString(fmt.Sprintf("flags:%d ", m.Flags))
	builder.WriteString(fmt.Sprintf("lsn:%s ", m.LSN))
	builder.WriteString(fmt.Sprintf("xid:%d ", m.Xid))
	builder.WriteString("}")
	return builder.String()
}
