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
	"github.com/jackc/pglogrepl"
)

func ParseXlogData(data []byte, lastTransactionId *uint32) (pglogrepl.Message, error) {
	// Normally unsupported message received
	var decoder pglogrepl.MessageDecoder
	msgType := pglogrepl.MessageType(data[0])
	switch msgType {
	case MessageTypeLogicalDecodingMessage:
		decoder = new(LogicalReplicationMessage)
	}
	if decoder != nil {
		if err := decoder.Decode(data[1:]); err != nil {
			return nil, err
		}

		// See if we have a transactional logical replication message, if so set the transaction id
		if msgType == MessageTypeLogicalDecodingMessage {
			if logRepMsg := decoder.(*LogicalReplicationMessage); logRepMsg.IsTransactional() {
				logRepMsg.Xid = func(xid uint32) *uint32 {
					return &xid
				}(*lastTransactionId)
			}
		}

		return decoder.(pglogrepl.Message), nil
	}

	return pglogrepl.Parse(data)
}
