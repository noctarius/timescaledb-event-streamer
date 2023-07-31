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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pglogrepl"
)

type baseMessage struct {
	msgType pglogrepl.MessageType
}

// Type returns message type.
func (m *baseMessage) Type() pglogrepl.MessageType {
	return m.msgType
}

func (m *baseMessage) SetType(
	t pglogrepl.MessageType,
) {

	m.msgType = t
}

func (m *baseMessage) Decode(
	_ []byte,
) error {

	return fmt.Errorf("message decode not implemented")
}

func (m *baseMessage) decodeStringError(
	name, field string,
) error {

	return fmt.Errorf("%s.%s decode string error", name, field)
}

func (m *baseMessage) decodeString(
	src []byte,
) (string, int) {

	end := bytes.IndexByte(src, byte(0))
	if end == -1 {
		return "", -1
	}
	// Trim the last null byte before converting it to a Golang string, then we can
	// compare the result string with a Golang string literal.
	return string(src[:end]), end + 1
}

func (m *baseMessage) decodeLSN(
	src []byte,
) (pglogrepl.LSN, int) {

	return pglogrepl.LSN(binary.BigEndian.Uint64(src)), 8
}
