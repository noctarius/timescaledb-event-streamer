package decoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pglogrepl"
	"time"
)

const (
	MessageTypeLogicalDecodingMessage pglogrepl.MessageType = 'M'
)

func ParseXlogData(data []byte, lastTransactionId *uint32) (pglogrepl.Message, error) {
	// Normally unsupported message received
	var decoder pglogrepl.MessageDecoder
	msgType := pglogrepl.MessageType(data[0])
	switch msgType {
	case MessageTypeLogicalDecodingMessage:
		decoder = new(LogicalReplicationMessage)
	}
	if decoder != nil {
		if err := decoder.Decode(data[1:]); err != nil {
			return nil, err
		}

		// See if we have a transactional logical replication message, if so set the transaction id
		if msgType == MessageTypeLogicalDecodingMessage {
			if logRepMsg := decoder.(*LogicalReplicationMessage); logRepMsg.IsTransactional() {
				logRepMsg.Xid = &(*lastTransactionId)
			}
		}

		return decoder.(pglogrepl.Message), nil
	}

	return pglogrepl.Parse(data)
}

// LogicalReplicationMessage is a logical replication message.
type LogicalReplicationMessage struct {
	baseMessage
	// Flags is either 0 (non-transactional) or 1 (transactional)
	Flags uint8
	// Xid is the transaction id (if transactional logical replication message)
	Xid *uint32
	// LSN is the LSN of the logical replication message
	LSN pglogrepl.LSN
	// Prefix is the prefix of the logical replication message
	Prefix string
	// Content is the content of the logical replication message
	Content []byte
}

func (m *LogicalReplicationMessage) Decode(src []byte) (err error) {
	var low, used int
	m.Flags = src[0]
	low += 1
	m.LSN, used = m.decodeLSN(src[low:])
	low += used
	m.Prefix, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("LogicalReplicationMessage", "Prefix")
	}
	low += used
	contentLength := binary.BigEndian.Uint32(src[low:])
	low += 4
	m.Content = src[low : low+int(contentLength)]

	m.SetType(MessageTypeLogicalDecodingMessage)

	return nil
}

func (m *LogicalReplicationMessage) IsTransactional() bool {
	return m.Flags == 1
}

type baseMessage struct {
	msgType pglogrepl.MessageType
}

// Type returns message type.
func (m *baseMessage) Type() pglogrepl.MessageType {
	return m.msgType
}

func (m *baseMessage) SetType(t pglogrepl.MessageType) {
	m.msgType = t
}

func (m *baseMessage) Decode(_ []byte) error {
	return fmt.Errorf("message decode not implemented")
}

func (m *baseMessage) lengthError(name string, expectedLen, actualLen int) error {
	return fmt.Errorf("%s must have %d bytes, got %d bytes", name, expectedLen, actualLen)
}

func (m *baseMessage) decodeStringError(name, field string) error {
	return fmt.Errorf("%s.%s decode string error", name, field)
}

func (m *baseMessage) decodeTupleDataError(name, field string, e error) error {
	return fmt.Errorf("%s.%s decode tuple error: %s", name, field, e.Error())
}

func (m *baseMessage) invalidTupleTypeError(name, field string, e string, a byte) error {
	return fmt.Errorf("%s.%s invalid tuple type value, expect %s, actual %c", name, field, e, a)
}

func (m *baseMessage) decodeString(src []byte) (string, int) {
	end := bytes.IndexByte(src, byte(0))
	if end == -1 {
		return "", -1
	}
	// Trim the last null byte before converting it to a Golang string, then we can
	// compare the result string with a Golang string literal.
	return string(src[:end]), end + 1
}

func (m *baseMessage) decodeLSN(src []byte) (pglogrepl.LSN, int) {
	return pglogrepl.LSN(binary.BigEndian.Uint64(src)), 8
}

func (m *baseMessage) decodeTime(src []byte) (time.Time, int) {
	return pgTimeToTime(int64(binary.BigEndian.Uint64(src))), 8
}

func (m *baseMessage) decodeUint16(src []byte) (uint16, int) {
	return binary.BigEndian.Uint16(src), 2
}

func (m *baseMessage) decodeUint32(src []byte) (uint32, int) {
	return binary.BigEndian.Uint32(src), 4
}

func (m *baseMessage) decodeInt32(src []byte) (int32, int) {
	asUint32, size := m.decodeUint32(src)
	return int32(asUint32), size
}

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func pgTimeToTime(microsecSinceY2K int64) time.Time {
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return time.Unix(0, microsecSinceUnixEpoch*1000)
}
