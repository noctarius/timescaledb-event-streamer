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
	"crypto/rand"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOffset_Write_Read(
	t *testing.T,
) {

	o1 := &Offset{
		Timestamp: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		LSN:       100000000,
	}

	d, err := o1.MarshalBinary()
	if err != nil {
		t.Error(err)
	}

	o2 := &Offset{}
	if err := o2.UnmarshalBinary(d); err != nil {
		t.Error(err)
	}

	assert.Equal(t, o1.Timestamp, o2.Timestamp)
	assert.Equal(t, o1.LSN, o2.LSN)
	assert.Equal(t, o1.Snapshot, o2.Snapshot)
	assert.Equal(t, o1.SnapshotDone, o2.SnapshotDone)
	assert.True(t, o1.Equal(o2))
}

func TestOffset_Write_Read_WithSnapshotKeyset_NoSnapshotName(
	t *testing.T,
) {

	snapshotKeyset := make([]byte, 50)
	if _, err := rand.Read(snapshotKeyset); err != nil {
		t.Error(err)
	}

	o1 := &Offset{
		Timestamp:      time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 1000,
		SnapshotKeyset: snapshotKeyset,
		LSN:            100000000,
	}

	d, err := o1.MarshalBinary()
	if err != nil {
		t.Error(err)
	}

	o2 := &Offset{}
	if err := o2.UnmarshalBinary(d); err != nil {
		t.Error(err)
	}

	assert.Equal(t, o1.Timestamp, o2.Timestamp)
	assert.Equal(t, o1.LSN, o2.LSN)
	assert.Equal(t, o1.Snapshot, o2.Snapshot)
	assert.Equal(t, o1.SnapshotOffset, o2.SnapshotOffset)
	assert.Equal(t, o1.SnapshotKeyset, o2.SnapshotKeyset)
	assert.Equal(t, o1.SnapshotDone, o2.SnapshotDone)
	assert.True(t, o1.Equal(o2))
}

func TestOffset_Write_Read_WithSnapshotKeyset_WithSnapshotName(
	t *testing.T,
) {

	snapshotKeyset := make([]byte, 50)
	if _, err := rand.Read(snapshotKeyset); err != nil {
		t.Error(err)
	}

	o1 := &Offset{
		Timestamp:      time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 1000,
		SnapshotName:   lo.ToPtr("wertzuiopasdfghjklyxcvbnm"),
		SnapshotKeyset: snapshotKeyset,
		LSN:            100000000,
	}

	d, err := o1.MarshalBinary()
	if err != nil {
		t.Error(err)
	}

	o2 := &Offset{}
	if err := o2.UnmarshalBinary(d); err != nil {
		t.Error(err)
	}

	assert.Equal(t, o1.Timestamp, o2.Timestamp)
	assert.Equal(t, o1.LSN, o2.LSN)
	assert.Equal(t, o1.Snapshot, o2.Snapshot)
	assert.Equal(t, o1.SnapshotOffset, o2.SnapshotOffset)
	assert.Equal(t, o1.SnapshotKeyset, o2.SnapshotKeyset)
	assert.Equal(t, o1.SnapshotDone, o2.SnapshotDone)
	assert.True(t, o1.Equal(o2))
}

func TestOffset_Write_Read_NoSnapshotKeyset_NoSnapshotName(
	t *testing.T,
) {

	o1 := &Offset{
		Timestamp:      time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 1000,
		LSN:            100000000,
	}

	d, err := o1.MarshalBinary()
	if err != nil {
		t.Error(err)
	}

	o2 := &Offset{}
	if err := o2.UnmarshalBinary(d); err != nil {
		t.Error(err)
	}

	assert.Equal(t, o1.Timestamp, o2.Timestamp)
	assert.Equal(t, o1.LSN, o2.LSN)
	assert.Equal(t, o1.Snapshot, o2.Snapshot)
	assert.Equal(t, o1.SnapshotOffset, o2.SnapshotOffset)
	assert.Equal(t, o1.SnapshotKeyset, o2.SnapshotKeyset)
	assert.Equal(t, o1.SnapshotDone, o2.SnapshotDone)
	assert.True(t, o1.Equal(o2))
}

func TestOffset_Write_Read_NoSnapshotKeyset_WithSnapshotName(
	t *testing.T,
) {

	o1 := &Offset{
		Timestamp:      time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 1000,
		SnapshotName:   lo.ToPtr("wertzuiopasdfghjklyxcvbnm"),
		LSN:            100000000,
	}

	d, err := o1.MarshalBinary()
	if err != nil {
		t.Error(err)
	}

	o2 := &Offset{}
	if err := o2.UnmarshalBinary(d); err != nil {
		t.Error(err)
	}

	assert.Equal(t, o1.Timestamp, o2.Timestamp)
	assert.Equal(t, o1.LSN, o2.LSN)
	assert.Equal(t, o1.Snapshot, o2.Snapshot)
	assert.Equal(t, o1.SnapshotOffset, o2.SnapshotOffset)
	assert.Equal(t, o1.SnapshotKeyset, o2.SnapshotKeyset)
	assert.Equal(t, o1.SnapshotDone, o2.SnapshotDone)
	assert.True(t, o1.Equal(o2))
}
