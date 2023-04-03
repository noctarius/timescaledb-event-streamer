package offset

import (
	"encoding/binary"
	"github.com/jackc/pglogrepl"
	"time"
)

type Offset struct {
	Timestamp      time.Time     `json:"timestamp"`
	Snapshot       bool          `json:"snapshot"`
	SnapshotOffset int           `json:"snapshot_offset"`
	LSN            pglogrepl.LSN `json:"lsn"`
}

func (o *Offset) UnmarshalBinary(data []byte) error {
	o.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[:8]))).In(time.UTC)
	o.Snapshot = data[8] == 1
	o.SnapshotOffset = int(binary.BigEndian.Uint32(data[9:]))
	o.LSN = pglogrepl.LSN(binary.BigEndian.Uint64(data[13:]))
	return nil
}

func (o *Offset) MarshalBinary() ([]byte, error) {
	data := make([]byte, 21)
	binary.BigEndian.PutUint64(data[:8], uint64(o.Timestamp.UnixNano()))
	data[8] = 0
	if o.Snapshot {
		data[8] = 1
	}
	binary.BigEndian.PutUint32(data[9:], uint32(o.SnapshotOffset))
	binary.BigEndian.PutUint64(data[13:], uint64(o.LSN))
	return data, nil
}

func (o *Offset) Equal(other *Offset) bool {
	if !o.Timestamp.Equal(other.Timestamp) {
		return false
	}

	if o.Snapshot != other.Snapshot {
		return false
	}

	if o.SnapshotOffset != other.SnapshotOffset {
		return false
	}

	return o.LSN == other.LSN
}
