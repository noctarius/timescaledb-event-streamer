package sysconfig

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/event/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/event/topic"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	"github.com/noctarius/timescaledb-event-streamer/internal/schema"
)

type EventEmitterProvider = func(schemaRegistry *schema.Registry, topicNameGenerator *topic.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor) (*sink.EventEmitter, error)

type SinkProvider = func() (sink.Sink, error)

type NameGeneratorProvider = func() (*topic.NameGenerator, error)

type NamingStrategyProvider = func() (topic.NamingStrategy, error)
