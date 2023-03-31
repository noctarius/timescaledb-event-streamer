package sysconfig

import (
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/event/topic"
	"github.com/noctarius/event-stream-prototype/internal/replication/transactional"
	"github.com/noctarius/event-stream-prototype/internal/schema"
)

type EventEmitterProvider = func(schemaRegistry *schema.Registry, topicNameGenerator *topic.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor) (*sink.EventEmitter, error)

type SinkProvider = func() (sink.Sink, error)

type NameGeneratorProvider = func() (*topic.NameGenerator, error)

type NamingStrategyProvider = func() (topic.NamingStrategy, error)
