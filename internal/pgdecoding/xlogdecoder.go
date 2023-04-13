package pgdecoding

import (
	"github.com/jackc/pglogrepl"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
)

func ParseXlogData(data []byte, lastTransactionId *uint32) (pglogrepl.Message, error) {
	// Normally unsupported message received
	var decoder pglogrepl.MessageDecoder
	msgType := pglogrepl.MessageType(data[0])
	switch msgType {
	case pgtypes.MessageTypeLogicalDecodingMessage:
		decoder = new(pgtypes.LogicalReplicationMessage)
	}
	if decoder != nil {
		if err := decoder.Decode(data[1:]); err != nil {
			return nil, err
		}

		// See if we have a transactional logical replication message, if so set the transaction id
		if msgType == pgtypes.MessageTypeLogicalDecodingMessage {
			if logRepMsg := decoder.(*pgtypes.LogicalReplicationMessage); logRepMsg.IsTransactional() {
				logRepMsg.Xid = func(xid uint32) *uint32 {
					return &xid
				}(*lastTransactionId)
			}
		}

		return decoder.(pglogrepl.Message), nil
	}

	return pglogrepl.Parse(data)
}
