package schema

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/topic/namegenerator"
	"github.com/reugn/async"
)

type Registry struct {
	topicNameGenerator namegenerator.NameGenerator
	schemaRegistry     map[string]schema.Struct
	mutex              *async.ReentrantLock
}

func NewRegistry(topicNameGenerator namegenerator.NameGenerator) schema.Registry {
	r := &Registry{
		topicNameGenerator: topicNameGenerator,
		schemaRegistry:     make(map[string]schema.Struct, 0),
		mutex:              async.NewReentrantLock(),
	}
	initializeSourceSchemas(r)
	return r
}

func (r *Registry) RegisterSchema(schemaName string, schema schema.Struct) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.schemaRegistry[schemaName] = schema
}

func (r *Registry) GetSchema(schemaName string) schema.Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.schemaRegistry[schemaName]
}

func (r *Registry) GetSchemaOrCreate(schemaName string, creator func() schema.Struct) schema.Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if schema, ok := r.schemaRegistry[schemaName]; ok {
		return schema
	}
	schema := creator()
	r.schemaRegistry[schemaName] = schema
	return schema
}

func (r *Registry) HypertableEnvelopeSchemaName(hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.Envelope", r.topicNameGenerator.SchemaTopicName(hypertable))
}

func (r *Registry) HypertableKeySchemaName(hypertable *systemcatalog.Hypertable) string {
	return fmt.Sprintf("%s.Key", r.topicNameGenerator.SchemaTopicName(hypertable))
}

func (r *Registry) MessageEnvelopeSchemaName() string {
	return fmt.Sprintf("%s.Envelope", r.topicNameGenerator.MessageTopicName())
}

func initializeSourceSchemas(registry schema.Registry) {
	registry.RegisterSchema(schema.SourceSchemaName, schema.SourceSchema())
	registry.RegisterSchema(schema.MessageValueSchemaName, schema.MessageValueSchema(registry))
	registry.RegisterSchema(schema.MessageKeySchemaName, schema.MessageKeySchema())
}
