package file

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime"
	"testing"
	"time"
)

func Test_Writing_Reading(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}

	f, err := os.CreateTemp("", "offset")
	if err != nil {
		t.FailNow()
	}
	defer os.Remove(f.Name())

	foo := &statestorage.Offset{
		Timestamp:      time.Date(2023, 01, 01, 0, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 1000,
		LSN:            pgtypes.LSN(1000000),
		SnapshotName:   supporting.AddrOf("foo-12345-12345"),
	}
	bar := &statestorage.Offset{
		Timestamp:      time.Date(2023, 01, 01, 1, 0, 0, 0, time.UTC),
		Snapshot:       true,
		SnapshotOffset: 2000,
		LSN:            pgtypes.LSN(2000000),
		SnapshotName:   supporting.AddrOf("bar-54321-54321"),
	}
	baz := &statestorage.Offset{
		Timestamp:      time.Date(2023, 02, 01, 1, 0, 0, 0, time.UTC),
		Snapshot:       false,
		SnapshotOffset: 3000,
		LSN:            pgtypes.LSN(3000000),
	}

	offsetStorage, err := NewFileStateStorage(f.Name())
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
	assert.NoError(t, err, "failed getting offsets")
	assert.Equal(t, 3, len(offsets), "offsets has unexpected length")
	assert.Equal(t, foo, offsets["foo"])
	assert.Equal(t, bar, offsets["bar"])
	assert.Equal(t, baz, offsets["baz"])

	err = offsetStorage.Stop()
	assert.NoError(t, err, "failed stopping FileOffsetStorage")

	secondOffsetStorage, err := NewFileStateStorage(f.Name())
	assert.NoError(t, err, "failed to instantiate FileOffsetStorage")

	err = secondOffsetStorage.Start()
	assert.NoError(t, err, "failed starting FileOffsetStorage")

	offsets, err = secondOffsetStorage.Get()
	assert.NoError(t, err, "failed getting offsets")
	assert.Equal(t, 3, len(offsets), "offsets has unexpected length")
	assert.Equal(t, foo, offsets["foo"])
	assert.Equal(t, bar, offsets["bar"])
	assert.Equal(t, baz, offsets["baz"])
}
