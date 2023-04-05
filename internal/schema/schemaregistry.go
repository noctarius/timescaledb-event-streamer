package schema

import "github.com/reugn/async"

type Registry struct {
	schemaRegistry map[string]Struct
	mutex          *async.ReentrantLock
}

func NewSchemaRegistry() *Registry {
	r := &Registry{
		schemaRegistry: make(map[string]Struct, 0),
		mutex:          async.NewReentrantLock(),
	}
	initializeSourceSchemas(r)
	return r
}

func (r *Registry) RegisterSchema(schemaName string, schema Struct) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.schemaRegistry[schemaName] = schema
}

func (r *Registry) GetSchema(schemaName string) Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.schemaRegistry[schemaName]
}

func (r *Registry) GetSchemaOrCreate(schemaName string, creator func() Struct) Struct {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if schema, ok := r.schemaRegistry[schemaName]; ok {
		return schema
	}
	schema := creator()
	r.schemaRegistry[schemaName] = schema
	return schema
}

func initializeSourceSchemas(registry *Registry) {
	registry.RegisterSchema(SourceSchemaName, sourceSchema())
	registry.RegisterSchema(MessageValueSchemaName, messageValueSchema(registry))
	registry.RegisterSchema(MessageKeySchemaName, messageKeySchema())
}
