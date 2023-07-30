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

package sink

import "encoding/binary"

type sinkContext struct {
	attributes          map[string]string
	transientAttributes map[string]string
}

func newSinkContext() *sinkContext {
	return &sinkContext{
		attributes:          make(map[string]string),
		transientAttributes: make(map[string]string),
	}
}

func (s *sinkContext) UnmarshalBinary(
	data []byte,
) error {

	offset := uint32(0)
	numOfItems := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	for i := uint32(0); i < numOfItems; i++ {
		keyLength := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		valueLength := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		key := string(data[offset : offset+keyLength])
		offset += keyLength

		value := string(data[offset : offset+valueLength])
		offset += valueLength

		s.SetAttribute(key, value)
	}
	return nil
}

func (s *sinkContext) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0)

	numOfItems := uint32(len(s.attributes))
	data = binary.BigEndian.AppendUint32(data, numOfItems)

	for key, value := range s.attributes {
		keyBytes := []byte(key)
		valueBytes := []byte(value)

		data = binary.BigEndian.AppendUint32(data, uint32(len(keyBytes)))
		data = binary.BigEndian.AppendUint32(data, uint32(len(valueBytes)))

		data = append(data, keyBytes...)
		data = append(data, valueBytes...)
	}
	return data, nil
}

func (s *sinkContext) SetTransientAttribute(
	key string, value string,
) {

	s.transientAttributes[key] = value
}

func (s *sinkContext) TransientAttribute(
	key string,
) (value string, present bool) {

	value, present = s.transientAttributes[key]
	return
}

func (s *sinkContext) SetAttribute(
	key string, value string,
) {

	s.attributes[key] = value
}

func (s *sinkContext) Attribute(
	key string,
) (value string, present bool) {

	value, present = s.attributes[key]
	return
}
