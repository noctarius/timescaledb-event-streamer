package testing

import "github.com/noctarius/timescaledb-event-streamer/internal/schema"

type Source struct {
	Connector string `json:"connector"`
	DB        string `json:"db"`
	LSN       string `json:"lsn"`
	TxId      uint32 `json:"txId"`
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Snapshot  bool   `json:"snapshot"`
	Table     string `json:"table"`
	TsMs      uint64 `json:"ts_ms"`
	Version   string `json:"version"`
}

type Payload struct {
	Before map[string]any            `json:"before"`
	After  map[string]any            `json:"after"`
	Op     schema.Operation          `json:"op"`
	TsdbOp schema.TimescaleOperation `json:"tsdb_op"`
	Source Source                    `json:"source"`
	TsMs   uint64                    `json:"ts_ms"`
}

type Field struct {
	Name     string  `json:"name"`
	Field    string  `json:"field"`
	Optional bool    `json:"optional"`
	Type     string  `json:"type"`
	Fields   []Field `json:"fields"`
	Default  any     `json:"default"`
}

type Schema struct {
	Fields   []Field `json:"fields"`
	Name     string  `json:"name"`
	Optional bool    `json:"optional"`
	Type     string  `json:"type"`
}

type Envelope struct {
	Payload Payload `json:"payload"`
	Schema  Schema  `json:"schema"`
}
