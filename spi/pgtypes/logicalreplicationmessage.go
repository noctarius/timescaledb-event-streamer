package pgtypes

import (
	"encoding/binary"
	"github.com/jackc/pglogrepl"
)

const (
	MessageTypeLogicalDecodingMessage pglogrepl.MessageType = 'M'
)

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
