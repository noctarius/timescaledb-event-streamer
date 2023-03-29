package offset

import (
	"github.com/jackc/pglogrepl"
	"time"
)

type Offset struct {
	Timestamp      time.Time     `json:"timestamp"`
	Snapshot       bool          `json:"snapshot"`
	SnapshotOffset int           `json:"snapshot_offset"`
	LSN            pglogrepl.LSN `json:"lsn"`
}
