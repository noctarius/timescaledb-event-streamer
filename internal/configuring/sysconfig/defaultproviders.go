package sysconfig

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/event/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/transactional"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namegenerator"
)

type EventEmitterProvider = func(schemaRegistry *schema.Registry, topicNameGenerator *namegenerator.NameGenerator,
	transactionMonitor *transactional.TransactionMonitor) (*sink.EventEmitter, error)

type NameGeneratorProvider = func() (*namegenerator.NameGenerator, error)
