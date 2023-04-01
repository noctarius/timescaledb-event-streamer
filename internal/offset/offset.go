package offset

import (
	"encoding/binary"
	"github.com/jackc/pglogrepl"
	"io"
	"time"
)

type Offset struct {
	Timestamp      time.Time     `json:"timestamp"`
	Snapshot       bool          `json:"snapshot"`
	SnapshotOffset int           `json:"snapshot_offset"`
	LSN            pglogrepl.LSN `json:"lsn"`
}

func (o *Offset) WriteBinary(writer io.Writer, endianness binary.ByteOrder) (int, error) {
	buf := make([]byte, 21)

	endianness.PutUint64(buf[:7], uint64(o.Timestamp.UnixNano()))

	buf[8] = 0
	if o.Snapshot {
		buf[8] = 1
	}

	endianness.PutUint32(buf[9:], uint32(o.SnapshotOffset))
	endianness.PutUint64(buf[13:], uint64(o.LSN))

	return writer.Write(buf)
}

func (o *Offset) ReadBinary(reader io.Reader, endianness binary.ByteOrder) (int, error) {
	buf := make([]byte, 21)
	n, err := reader.Read(buf)
	if err != nil {
		return -1, err
	}

	o.Timestamp = time.Unix(0, int64(endianness.Uint64(buf[:7])))
	o.Snapshot = buf[8] == 1
	o.SnapshotOffset = int(endianness.Uint32(buf[9:]))
	o.LSN = pglogrepl.LSN(endianness.Uint64(buf[13:]))

	return n, nil
}
