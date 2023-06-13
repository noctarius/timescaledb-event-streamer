package snapshotting

import (
	"bytes"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type SnapshotContext struct {
	snapshotName string
	complete     bool
	watermarks   map[string]*Watermark
}

func (sc *SnapshotContext) GetWatermark(hypertable *systemcatalog.Hypertable) (watermark *Watermark, present bool) {
	w, present := sc.watermarks[hypertable.CanonicalName()]
	if !present {
		return nil, false
	}
	return w, true
}

func (sc *SnapshotContext) GetOrCreateWatermark(
	hypertable *systemcatalog.Hypertable) (watermark *Watermark, created bool) {

	w, present := sc.watermarks[hypertable.CanonicalName()]
	if !present {
		w = newWatermark(hypertable)
		sc.watermarks[hypertable.CanonicalName()] = w
	}
	return w, !present
}

func (sc *SnapshotContext) MarshalBinary() (data []byte, err error) {
	buffer := encoding.NewWriteBuffer(1024)

	if err := buffer.PutString(sc.snapshotName); err != nil {
		return nil, err
	}

	if err := buffer.PutBool(sc.complete); err != nil {
		return nil, err
	}

	if err := buffer.PutUint32(uint32(len(sc.watermarks))); err != nil {
		return nil, err
	}

	for tableName, watermark := range sc.watermarks {
		if err := buffer.PutString(tableName); err != nil {
			return nil, err
		}

		if err := buffer.PutBool(watermark.complete); err != nil {
			return nil, err
		}

		if err := buffer.PutUint32(uint32(len(watermark.dataTypes))); err != nil {
			return nil, err
		}

		for column, dataType := range watermark.dataTypes {
			if err := buffer.PutString(column); err != nil {
				return nil, err
			}

			if err := buffer.PutUint32(dataType); err != nil {
				return nil, err
			}
		}

		for column, value := range watermark.high {
			if err := buffer.PutString(column); err != nil {
				return nil, err
			}

			if dataType, ok := watermark.dataTypes[column]; ok {
				if err := systemcatalog.BinaryMarshall(buffer, dataType, value); err != nil {
					return nil, err
				}
			} else {
				return nil, errors.Errorf("cannot find dataType (oid) for columns %s", column)
			}
		}

		if err := buffer.PutBool(watermark.low != nil); err != nil {
			return nil, err
		}
		if watermark.low != nil {
			for column, value := range watermark.low {
				if err := buffer.PutString(column); err != nil {
					return nil, err
				}

				if dataType, ok := watermark.dataTypes[column]; ok {
					if err := systemcatalog.BinaryMarshall(buffer, dataType, value); err != nil {
						return nil, err
					}
				} else {
					return nil, errors.Errorf("cannot find dataType (oid) for columns %s", column)
				}
			}
		}
	}
	return buffer.Bytes(), nil
}

func (sc *SnapshotContext) UnmarshalBinary(data []byte) error {
	buffer := encoding.NewReadBuffer(bytes.NewBuffer(data))

	snapshotName, err := buffer.ReadString()
	if err != nil {
		return err
	}
	sc.snapshotName = snapshotName

	complete, err := buffer.ReadBool()
	if err != nil {
		return err
	}
	sc.complete = complete

	numOfWatermarks, err := buffer.ReadUint32()
	if err != nil {
		return err
	}

	for i := uint32(0); i < numOfWatermarks; i++ {
		tableName, err := buffer.ReadString()
		if err != nil {
			return err
		}

		complete, err = buffer.ReadBool()
		if err != nil {
			return err
		}

		numOfDataTypes, err := buffer.ReadUint32()
		if err != nil {
			return err
		}

		dataTypes := make(map[string]uint32, numOfDataTypes)
		for o := uint32(0); o < numOfDataTypes; o++ {
			column, err := buffer.ReadString()
			if err != nil {
				return err
			}

			oid, err := buffer.ReadUint32()
			if err != nil {
				return err
			}

			dataTypes[column] = oid
		}

		high := make(map[string]any, numOfDataTypes)
		for o := uint32(0); o < numOfDataTypes; o++ {
			column, err := buffer.ReadString()
			if err != nil {
				return err
			}

			if oid, ok := dataTypes[column]; ok {
				val, err := systemcatalog.BinaryUnmarshall(buffer, oid)
				if err != nil {
					return err
				}

				high[column] = val
			} else {
				return errors.Errorf("cannot find dataType (oid) for columns %s", column)
			}
		}

		lowWatermarkAvailable, err := buffer.ReadBool()
		if err != nil {
			return err
		}

		var low map[string]any
		if lowWatermarkAvailable {
			low = make(map[string]any, numOfDataTypes)
			for o := uint32(0); o < numOfDataTypes; o++ {
				column, err := buffer.ReadString()
				if err != nil {
					return err
				}

				if oid, ok := dataTypes[column]; ok {
					val, err := systemcatalog.BinaryUnmarshall(buffer, oid)
					if err != nil {
						return err
					}

					low[column] = val
				} else {
					return errors.Errorf("cannot find dataType (oid) for columns %s", column)
				}
			}
		}

		if sc.watermarks == nil {
			sc.watermarks = make(map[string]*Watermark)
		}

		sc.watermarks[tableName] = &Watermark{
			complete:  complete,
			dataTypes: dataTypes,
			high:      high,
			low:       low,
		}
	}
	return nil
}

type Watermark struct {
	complete  bool
	dataTypes map[string]uint32
	high      map[string]any
	low       map[string]any
}

func newWatermark(hypertable *systemcatalog.Hypertable) *Watermark {
	dataTypes := make(map[string]uint32)
	if index, ok := hypertable.Columns().SnapshotIndex(); ok {
		for _, column := range index.Columns() {
			dataTypes[column.Name()] = column.DataType()
		}
	}

	return &Watermark{
		complete:  false,
		dataTypes: dataTypes,
		high:      make(map[string]any),
		low:       make(map[string]any),
	}
}

func (w *Watermark) Complete() bool {
	return w.complete
}

func (w *Watermark) MarkComplete() {
	w.complete = true
}

func (w *Watermark) HighWatermark() map[string]any {
	return w.high
}

func (w *Watermark) SetHighWatermark(values map[string]any) {
	w.high = values
}

func (w *Watermark) LowWatermark() map[string]any {
	return w.low
}

func (w *Watermark) SetLowWatermark(values map[string]any) {
	w.low = values
}

func (w *Watermark) DataTypes() map[string]uint32 {
	return w.dataTypes
}
