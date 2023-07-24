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

package watermark

import (
	"bytes"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type SnapshotContext struct {
	snapshotName string
	complete     bool
	watermarks   map[string]*Watermark
}

func NewSnapshotContext(
	snapshotName string,
) *SnapshotContext {

	return &SnapshotContext{
		snapshotName: snapshotName,
		complete:     false,
		watermarks:   make(map[string]*Watermark),
	}
}

func (sc *SnapshotContext) GetWatermark(
	hypertable *systemcatalog.Hypertable,
) (watermark *Watermark, present bool) {

	w, present := sc.watermarks[hypertable.CanonicalName()]
	if !present {
		return nil, false
	}
	return w, true
}

func (sc *SnapshotContext) GetOrCreateWatermark(
	hypertable *systemcatalog.Hypertable,
) (watermark *Watermark, created bool) {

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
				if err := pgtypes.BinaryMarshall(buffer, dataType, value); err != nil {
					return nil, err
				}
			} else {
				return nil, errors.Errorf("cannot find dataType (oid) for columns %s", column)
			}
		}

		if err := buffer.PutBool(watermark.HasValidLowWatermark()); err != nil {
			return nil, err
		}
		if watermark.HasValidLowWatermark() {
			for column, value := range watermark.low {
				if err := buffer.PutString(column); err != nil {
					return nil, err
				}

				if dataType, ok := watermark.dataTypes[column]; ok {
					if err := pgtypes.BinaryMarshall(buffer, dataType, value); err != nil {
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

func (sc *SnapshotContext) UnmarshalBinary(
	data []byte,
) error {

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
				val, err := pgtypes.BinaryUnmarshall(buffer, oid)
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
					val, err := pgtypes.BinaryUnmarshall(buffer, oid)
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

func newWatermark(
	hypertable *systemcatalog.Hypertable,
) *Watermark {

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

func (w *Watermark) MarkIncomplete() {
	w.complete = false
}

func (w *Watermark) HighWatermark() map[string]any {
	return w.high
}

func (w *Watermark) SetHighWatermark(
	values map[string]any,
) {

	w.high = values
}

func (w *Watermark) LowWatermark() map[string]any {
	return w.low
}

func (w *Watermark) SetLowWatermark(
	values map[string]any,
) {

	w.low = values
	w.checkComplete()
}

func (w *Watermark) HasValidLowWatermark() bool {
	return w.low != nil && len(w.low) > 0
}

func (w *Watermark) DataTypes() map[string]uint32 {
	return w.dataTypes
}

func (w *Watermark) checkComplete() {
	complete := true
	for key, value := range w.high {
		value2, present := w.low[key]
		if !present {
			complete = false
			break
		}

		if value != value2 {
			complete = false
			break
		}
	}
	w.complete = complete
}
