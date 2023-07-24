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

package encoding

import (
	"bytes"
	"encoding/binary"
	"github.com/go-errors/errors"
	"math"
)

type WriteBuffer interface {
	PutBit(
		val bool,
	) error
	PutBool(
		val bool,
	) error
	PutInt8(
		val int8,
	) error
	PutInt16(
		val int16,
	) error
	PutInt32(
		val int32,
	) error
	PutInt64(
		val int64,
	) error
	PutUint8(
		val uint8,
	) error
	PutUint16(
		val uint16,
	) error
	PutUint32(
		val uint32,
	) error
	PutUint64(
		val uint64,
	) error
	PutFloat32(
		val float32,
	) error
	PutFloat64(
		val float64,
	) error
	PutString(
		val string,
	) error
	PutBytes(
		val []byte,
	) error
	Length() int
	Bytes() []byte
}

type ReadBuffer interface {
	ReadBit() (bool, error)
	ReadBool() (bool, error)
	ReadInt8() (int8, error)
	ReadInt16() (int16, error)
	ReadInt32() (int32, error)
	ReadInt64() (int64, error)
	ReadUint8() (uint8, error)
	ReadUint16() (uint16, error)
	ReadUint32() (uint32, error)
	ReadUint64() (uint64, error)
	ReadFloat32() (float32, error)
	ReadFloat64() (float64, error)
	ReadString() (string, error)
	ReadBytes() ([]byte, error)
}

func NewWriteBuffer(
	initialCapacity int,
) WriteBuffer {

	buffer := &bytes.Buffer{}
	buffer.Grow(initialCapacity)
	return &writeBuffer{
		buffer: buffer,
	}
}

type writeBuffer struct {
	buffer *bytes.Buffer
}

func (w *writeBuffer) PutBit(
	val bool,
) error {

	v := byte(0)
	if val {
		v = 1
	}
	if err := w.buffer.WriteByte(v); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (w *writeBuffer) PutBool(
	val bool,
) error {

	v := byte(0)
	if val {
		v = 1
	}
	if err := w.buffer.WriteByte(v); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (w *writeBuffer) PutInt8(
	val int8,
) error {

	return w.PutUint8(uint8(val))
}

func (w *writeBuffer) PutInt16(
	val int16,
) error {

	return w.PutUint16(uint16(val))
}

func (w *writeBuffer) PutInt32(
	val int32,
) error {

	return w.PutUint32(uint32(val))
}

func (w *writeBuffer) PutInt64(
	val int64,
) error {

	return w.PutUint64(uint64(val))
}

func (w *writeBuffer) PutUint8(
	val uint8,
) error {

	if err := w.buffer.WriteByte(val); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (w *writeBuffer) PutUint16(
	val uint16,
) error {

	d := make([]byte, 2)
	binary.BigEndian.PutUint16(d, val)
	if _, err := w.buffer.Write(d); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (w *writeBuffer) PutUint32(
	val uint32,
) error {

	d := make([]byte, 4)
	binary.BigEndian.PutUint32(d, val)
	if _, err := w.buffer.Write(d); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (w *writeBuffer) PutUint64(
	val uint64,
) error {

	d := make([]byte, 8)
	binary.BigEndian.PutUint64(d, val)
	if _, err := w.buffer.Write(d); err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}

func (w *writeBuffer) PutFloat32(
	val float32,
) error {

	return w.PutUint32(math.Float32bits(val))
}

func (w *writeBuffer) PutFloat64(
	val float64,
) error {

	return w.PutUint64(math.Float64bits(val))
}

func (w *writeBuffer) PutString(
	val string,
) error {

	d := []byte(val)
	length := uint32(len(d))
	if err := w.PutUint32(length); err != nil {
		return err
	}
	if _, err := w.buffer.Write(d); err != nil {
		return err
	}
	return nil
}

func (w *writeBuffer) PutBytes(
	val []byte,
) error {

	length := uint32(len(val))
	if err := w.PutUint32(length); err != nil {
		return err
	}
	if _, err := w.buffer.Write(val); err != nil {
		return err
	}
	return nil
}

func (w *writeBuffer) Length() int {
	return w.buffer.Len()
}

func (w *writeBuffer) Bytes() []byte {
	return w.buffer.Bytes()
}

func NewReadBuffer(
	buffer *bytes.Buffer,
) ReadBuffer {

	return &readBuffer{buffer: buffer}
}

type readBuffer struct {
	buffer *bytes.Buffer
}

func (r *readBuffer) ReadBit() (bool, error) {
	return r.ReadBool()
}

func (r *readBuffer) ReadBool() (bool, error) {
	b, err := r.buffer.ReadByte()
	if err != nil {
		return false, err
	}
	return b == 1, nil
}

func (r *readBuffer) ReadInt8() (int8, error) {
	v, err := r.ReadUint8()
	if err != nil {
		return 0, err
	}
	return int8(v), nil
}

func (r *readBuffer) ReadInt16() (int16, error) {
	v, err := r.ReadUint16()
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

func (r *readBuffer) ReadInt32() (int32, error) {
	v, err := r.ReadUint32()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func (r *readBuffer) ReadInt64() (int64, error) {
	v, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

func (r *readBuffer) ReadUint8() (uint8, error) {
	b, err := r.buffer.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
}

func (r *readBuffer) ReadUint16() (uint16, error) {
	d := make([]byte, 2)
	if _, err := r.buffer.Read(d); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(d), nil
}

func (r *readBuffer) ReadUint32() (uint32, error) {
	d := make([]byte, 4)
	if _, err := r.buffer.Read(d); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(d), nil
}

func (r *readBuffer) ReadUint64() (uint64, error) {
	d := make([]byte, 8)
	if _, err := r.buffer.Read(d); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(d), nil
}

func (r *readBuffer) ReadFloat32() (float32, error) {
	v, err := r.ReadUint32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

func (r *readBuffer) ReadFloat64() (float64, error) {
	v, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

func (r *readBuffer) ReadString() (string, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return "", err
	}
	d := make([]byte, length)
	if _, err := r.buffer.Read(d); err != nil {
		return "", errors.Wrap(err, 0)
	}
	return string(d), nil
}

func (r *readBuffer) ReadBytes() ([]byte, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	d := make([]byte, length)
	if _, err := r.buffer.Read(d); err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return d, nil
}
