package storage

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/event-stream-prototype/internal/offset"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func Test_Writing_Reading(t *testing.T) {
	f, err := os.CreateTemp("", "offset")
	if err != nil {
		t.FailNow()
	}
	defer os.Remove(f.Name())

	foo := &offset.Offset{
		Timestamp:      time.Date(2023, 01, 01, 0, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 1000,
		LSN:            pglogrepl.LSN(1000000),
	}
	bar := &offset.Offset{
		Timestamp:      time.Date(2023, 01, 01, 1, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 2000,
		LSN:            pglogrepl.LSN(2000000),
	}
	baz := &offset.Offset{
		Timestamp:      time.Date(2023, 02, 01, 1, 0, 0, 0, time.UTC),
		Snapshot:       false,
		SnapshotOffset: 3000,
		LSN:            pglogrepl.LSN(3000000),
	}

	offsetStorage, err := NewFileOffsetStorage(f.Name())
	assert.NoError(t, err, "failed to instantiate FileOffsetStorage")

	err = offsetStorage.Start()
	assert.NoError(t, err, "failed starting FileOffsetStorage")

	err = offsetStorage.Set("foo", foo)
	assert.NoError(t, err, "failed setting foo")
	err = offsetStorage.Set("bar", bar)
	assert.NoError(t, err, "failed setting bar")
	err = offsetStorage.Set("baz", baz)
	assert.NoError(t, err, "failed setting baz")

	err = offsetStorage.Save()
	assert.NoError(t, err, "failed saving offsets")

	offsets, err := offsetStorage.Get()
	assert.Equal(t, 3, len(offsets), "offsets has unexpected length")
	assert.Equal(t, foo, offsets["foo"])
	assert.Equal(t, bar, offsets["bar"])
	assert.Equal(t, baz, offsets["baz"])

	err = offsetStorage.Stop()
	assert.NoError(t, err, "failed stopping FileOffsetStorage")

	secondOffsetStorage, err := NewFileOffsetStorage(f.Name())
	assert.NoError(t, err, "failed to instantiate FileOffsetStorage")

	err = secondOffsetStorage.Start()
	assert.NoError(t, err, "failed starting FileOffsetStorage")

	offsets, err = secondOffsetStorage.Get()
	assert.Equal(t, 3, len(offsets), "offsets has unexpected length")
	assert.Equal(t, foo, offsets["foo"])
	assert.Equal(t, bar, offsets["bar"])
	assert.Equal(t, baz, offsets["baz"])
}
