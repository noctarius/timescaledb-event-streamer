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

package statestorage

import (
	"bytes"
	"encoding/binary"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/samber/lo"
	"time"
)

type Offset struct {
	Timestamp      time.Time   `json:"timestamp"`
	Snapshot       bool        `json:"snapshot"`
	SnapshotDone   bool        `json:"snapshot_done"`
	SnapshotName   *string     `json:"snapshot_name,omitempty"`
	SnapshotOffset int         `json:"snapshot_offset"`
	SnapshotKeyset []byte      `json:"snapshot_keyset"`
	LSN            pgtypes.LSN `json:"lsn"`
}

func (o *Offset) UnmarshalBinary(
	data []byte,
) error {

	o.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[:8]))).In(time.UTC)
	o.Snapshot = data[8] == 1
	o.SnapshotOffset = int(binary.BigEndian.Uint32(data[9:]))
	o.LSN = pgtypes.LSN(binary.BigEndian.Uint64(data[13:]))

	offset := 21
	hasSnapshotName := false
	if data[offset] == 1 {
		hasSnapshotName = true
	}
	offset++
	if hasSnapshotName {
		snapshotNameLength := int(data[offset])
		offset++
		if snapshotNameLength > 0 {
			o.SnapshotName = lo.ToPtr(string(data[offset : offset+snapshotNameLength]))
			offset += snapshotNameLength
		}
	}
	o.SnapshotDone = data[offset] == 1
	offset++
	if len(data) >= offset+1 {
		length := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		o.SnapshotKeyset = make([]byte, length)
		copy(o.SnapshotKeyset, data[offset:])
		offset += int(length)
	}
	return nil
}

func (o *Offset) MarshalBinary() ([]byte, error) {
	size := 23
	if o.SnapshotName != nil {
		size++
		size += len([]byte(*o.SnapshotName))
	}
	if o.SnapshotKeyset != nil && len(o.SnapshotKeyset) > 0 {
		size += 4 // Add byte array length
		size += len(o.SnapshotKeyset)
	}
	data := make([]byte, size)
	binary.BigEndian.PutUint64(data[:8], uint64(o.Timestamp.UnixNano()))
	data[8] = 0
	if o.Snapshot {
		data[8] = 1
	}
	binary.BigEndian.PutUint32(data[9:], uint32(o.SnapshotOffset))
	binary.BigEndian.PutUint64(data[13:], uint64(o.LSN))

	offset := 21
	data[offset] = 1
	if o.SnapshotName == nil {
		data[offset] = 0
	}
	offset++
	if o.SnapshotName != nil {
		snapshotName := []byte(*o.SnapshotName)
		snapshotNameLength := len(snapshotName)
		data[offset] = byte(snapshotNameLength)
		offset++
		copy(data[offset:], snapshotName)
		offset += len(snapshotName)
	}
	data[offset] = 0
	if o.SnapshotDone {
		data[offset] = 1
	}
	offset++
	if o.SnapshotKeyset != nil && len(o.SnapshotKeyset) > 0 {
		length := len(o.SnapshotKeyset)
		binary.BigEndian.PutUint32(data[offset:], uint32(length))
		offset += 4
		copy(data[offset:], o.SnapshotKeyset)
		offset += length
	}
	return data, nil
}

func (o *Offset) Equal(
	other *Offset,
) bool {

	if !o.Timestamp.Equal(other.Timestamp) {
		return false
	}

	if o.Snapshot != other.Snapshot {
		return false
	}

	if o.SnapshotDone != other.SnapshotDone {
		return false
	}

	if o.SnapshotOffset != other.SnapshotOffset {
		return false
	}

	if (o.SnapshotName == nil && other.SnapshotName != nil) ||
		(o.SnapshotName != nil && other.SnapshotName == nil) ||
		(o.SnapshotName != nil && other.SnapshotName != nil && *o.SnapshotName != *other.SnapshotName) {
		return false
	}

	if (o.SnapshotKeyset == nil && other.SnapshotKeyset != nil) ||
		(o.SnapshotKeyset != nil && other.SnapshotKeyset == nil) ||
		(o.SnapshotKeyset != nil && other.SnapshotKeyset != nil &&
			(len(o.SnapshotKeyset) != len(other.SnapshotKeyset) ||
				!bytes.Equal(o.SnapshotKeyset, other.SnapshotKeyset))) {
		return false
	}

	return o.LSN == other.LSN
}
