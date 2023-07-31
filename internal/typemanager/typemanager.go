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

package typemanager

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/containers"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"reflect"
	"sync"
)

var (
	int8Type    = reflect.TypeOf(int8(0))
	int16Type   = reflect.TypeOf(int16(0))
	int32Type   = reflect.TypeOf(int32(0))
	int64Type   = reflect.TypeOf(int64(0))
	float32Type = reflect.TypeOf(float32(0))
	float64Type = reflect.TypeOf(float64(0))
	booleanType = reflect.TypeOf(true)
	stringType  = reflect.TypeOf("")
	byteaType   = reflect.TypeOf([]byte{})
	mapType     = reflect.TypeOf(map[string]any{})
)

type typeMapTypeFactory func(typeMap *pgtype.Map, typ pgtypes.PgType) *pgtype.Type

type typeRegistration struct {
	schemaType         schema.Type
	schemaBuilder      schema.Builder
	isArray            bool
	oidElement         uint32
	converter          pgtypes.TypeConverter
	codec              pgtype.Codec
	typeMapTypeFactory typeMapTypeFactory
}

// errIllegalValue represents an illegal type conversion request
// for the given value
var errIllegalValue = fmt.Errorf("illegal value for data type conversion")

type typeManager struct {
	logger      *logging.Logger
	sideChannel sidechannel.SideChannel

	typeMap *pgtype.Map

	typeCache      map[uint32]pgtypes.PgType
	typeNameCache  map[string]uint32
	typeCacheMutex sync.RWMutex

	optimizedTypes      map[uint32]pgtypes.PgType
	optimizedConverters map[uint32]typeRegistration
	dynamicConverters   map[uint32]typeRegistration
	cachedDecoderPlans  *containers.ConcurrentMap[uint32, pgtypes.TupleDecoderPlan]
}

func NewTypeManager(
	sideChannel sidechannel.SideChannel,
) (pgtypes.TypeManager, error) {

	logger, err := logging.NewLogger("TypeManager")
	if err != nil {
		return nil, err
	}

	typeManager := &typeManager{
		logger:      logger,
		sideChannel: sideChannel,

		typeMap: pgtype.NewMap(),

		typeCache:      make(map[uint32]pgtypes.PgType),
		typeNameCache:  make(map[string]uint32),
		typeCacheMutex: sync.RWMutex{},

		optimizedTypes:      make(map[uint32]pgtypes.PgType),
		optimizedConverters: make(map[uint32]typeRegistration),
		dynamicConverters:   make(map[uint32]typeRegistration),
		cachedDecoderPlans:  containers.NewConcurrentMap[uint32, pgtypes.TupleDecoderPlan](),
	}

	if err := typeManager.initialize(); err != nil {
		return nil, err
	}
	return typeManager, nil
}

func (tm *typeManager) initialize() error {
	tm.typeCacheMutex.Lock()
	defer tm.typeCacheMutex.Unlock()

	if err := tm.sideChannel.ReadPgTypes(tm.typeFactory, tm.registerType); err != nil {
		return err
	}
	return nil
}

func (tm *typeManager) typeFactory(
	namespace, name string, kind pgtypes.PgKind, oid uint32, category pgtypes.PgCategory,
	arrayType, recordType bool, oidArray uint32, oidElement uint32,
	oidParent uint32, modifiers int, enumValues []string, delimiter string,
) pgtypes.PgType {

	return newType(tm, namespace, name, kind, oid, category, arrayType, recordType,
		oidArray, oidElement, oidParent, modifiers, enumValues, delimiter)
}

func (tm *typeManager) ResolveDataType(
	oid uint32,
) (pgtypes.PgType, error) {

	get := func() (pgtypes.PgType, bool) {
		tm.typeCacheMutex.RLock()
		defer tm.typeCacheMutex.RUnlock()

		dataType, present := tm.typeCache[oid]
		if present {
			return dataType, true
		}
		return nil, false
	}

	resolve := func() (bool, error) {
		tm.typeCacheMutex.Lock()
		defer tm.typeCacheMutex.Unlock()

		var pt pgtypes.PgType
		err := tm.sideChannel.ReadPgTypes(tm.typeFactory, func(typ pgtypes.PgType) error {
			pt = typ
			return tm.registerType(typ)
		}, oid)

		if err != nil {
			return false, err
		}

		if pt == nil {
			return false, nil
		}
		return true, nil
	}

	// Is it already available / cached?
	t, present := get()
	if present {
		return t, nil
	}

	// Not yet available, needs to be resolved
	found, err := resolve()
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.Errorf("illegal oid: %d", oid)
	}

	t, present = get()
	if !present {
		panic("illegal state, PgType not available after successful resolve")
	}

	return t, nil
}

func (tm *typeManager) ResolveTypeConverter(
	oid uint32,
) (pgtypes.TypeConverter, error) {

	if registration, present := coreTypes[oid]; present {
		return registration.converter, nil
	}
	if registration, present := tm.optimizedConverters[oid]; present {
		return registration.converter, nil
	}
	if registration, present := tm.dynamicConverters[oid]; present {
		return registration.converter, nil
	}
	return nil, fmt.Errorf("unsupported OID: %d", oid)
}

func (tm *typeManager) NumKnownTypes() int {
	tm.typeCacheMutex.RLock()
	defer tm.typeCacheMutex.RUnlock()
	return len(tm.typeCache)
}

func (tm *typeManager) OidByName(
	name string,
) uint32 {

	tm.typeCacheMutex.RLock()
	defer tm.typeCacheMutex.RUnlock()
	oid, present := tm.typeNameCache[name]
	if !present {
		panic(fmt.Sprintf("Type %s isn't registered", name))
	}
	return oid
}

func (tm *typeManager) DecodeTuples(
	relation *pgtypes.RelationMessage, tupleData *pglogrepl.TupleData,
) (map[string]any, error) {

	plan, err := tm.GetOrPlanTupleDecoder(relation)
	if err != nil {
		return nil, err
	}
	return plan.Decode(tupleData)
}

func (tm *typeManager) GetOrPlanTupleDecoder(
	relation *pgtypes.RelationMessage,
) (plan pgtypes.TupleDecoderPlan, err error) {

	plan, ok := tm.cachedDecoderPlans.Load(relation.RelationID)
	if !ok {
		plan, err = planTupleDecoder(tm, relation)
		if err != nil {
			return nil, err
		}
		tm.cachedDecoderPlans.Store(relation.RelationID, plan)
	}
	return plan, nil
}

func (tm *typeManager) GetOrPlanRowDecoder(
	fields []pgconn.FieldDescription,
) (pgtypes.RowDecoder, error) {

	return newRowDecoder(tm, fields)
}

func (tm *typeManager) getSchemaType(
	oid uint32, arrayType bool, kind pgtypes.PgKind,
) schema.Type {

	if registration, present := coreTypes[oid]; present {
		return registration.schemaType
	}
	if registration, present := tm.optimizedConverters[oid]; present {
		return registration.schemaType
	}
	if arrayType {
		return schema.ARRAY
	} else if kind == pgtypes.EnumKind {
		return schema.STRING
	}
	return schema.STRUCT
}

func (tm *typeManager) resolveSchemaBuilder(
	pgType *pgType,
) schema.Builder {

	if registration, present := tm.optimizedConverters[pgType.oid]; present {
		if registration.schemaBuilder != nil {
			return registration.schemaBuilder
		}
	}

	switch pgType.schemaType {
	case schema.INT8:
		return schema.Int8()

	case schema.INT16:
		return schema.Int16()

	case schema.INT32:
		return schema.Int32()

	case schema.INT64:
		return schema.Int64()

	case schema.FLOAT32:
		return schema.Float32()

	case schema.FLOAT64:
		return schema.Float64()

	case schema.BOOLEAN:
		return schema.Boolean()

	case schema.STRING:
		if pgType.kind == pgtypes.EnumKind {
			return schema.Enum(pgType.EnumValues())
		}
		switch pgType.oid {
		case pgtype.JSONOID, pgtype.JSONBOID:
			return schema.Json()

		case pgtype.UUIDOID:
			return schema.Uuid()

		case pgtype.BitOID:
			// TODO: needs better handling

		case 142: // XML
			return schema.Xml()
		}
		return schema.String()

	case schema.BYTES:
		return schema.Bytes()

	case schema.ARRAY:
		elementType := pgType.ElementType()
		return schema.NewSchemaBuilder(pgType.schemaType).ValueSchema(elementType.SchemaBuilder())

	case schema.MAP:
		return nil

	default:
		return nil
	}
}

func (tm *typeManager) registerType(
	typ pgtypes.PgType,
) error {

	tm.typeCache[typ.Oid()] = typ
	tm.typeNameCache[typ.Name()] = typ.Oid()

	// Is core type not available in TypeMap by default (bug or not implemented in pgx)?
	if registration, present := coreTypes[typ.Oid()]; present {
		if !tm.knownInTypeMap(typ.Oid()) {
			if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
				return err
			}
		}
	}

	// Optimized types have dynamic OIDs and need to registered dynamically
	if registration, present := optimizedTypes[typ.Name()]; present {
		if t, ok := typ.(*pgType); ok {
			t.schemaType = registration.schemaType
		}

		tm.optimizedTypes[typ.Oid()] = typ

		converter := tm.resolveOptimizedTypeConverter(typ, registration)
		if converter == nil {
			return errors.Errorf("Type %s has no assigned value converter", typ.Name())
		}

		tm.optimizedConverters[typ.Oid()] = typeRegistration{
			schemaType:    registration.schemaType,
			schemaBuilder: registration.schemaBuilder,
			isArray:       registration.isArray,
			oidElement:    typ.OidElement(),
			converter:     converter,
			codec:         registration.codec,
		}

		if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
			return err
		}
	}

	// Enums are user defined objects and need to be registered manually
	if typ.Kind() == pgtypes.EnumKind {
		registration := typeRegistration{
			schemaType: typ.SchemaType(),
			converter:  enum2string,
			codec:      &pgtype.EnumCodec{},
		}

		tm.dynamicConverters[typ.Oid()] = registration
		if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
			return err
		}
	}

	// Object types (all remaining) need to be handled specifically
	if typ.SchemaType() == schema.STRUCT {
		// TODO: ignore for now - missing implementation
		registration := typeRegistration{
			schemaType: typ.SchemaType(),
		}
		tm.dynamicConverters[typ.Oid()] = registration
	}

	return nil
}

func (tm *typeManager) knownInTypeMap(
	oid uint32,
) (known bool) {

	_, known = tm.typeMap.TypeForOID(oid)
	return
}

func (tm *typeManager) registerTypeInTypeMap(
	typ pgtypes.PgType, registration typeRegistration,
) error {

	// If specific codec is registered, we can use it directly
	if registration.codec != nil {
		tm.typeMap.RegisterType(&pgtype.Type{Name: typ.Name(), OID: typ.Oid(), Codec: registration.codec})
		return nil
	}

	// Slightly more complicated types have a factory for the pgx type
	if registration.typeMapTypeFactory != nil {
		tm.typeMap.RegisterType(registration.typeMapTypeFactory(tm.typeMap, typ))
		return nil
	}

	// When array type, try to resolve element type and use generic array codec
	if typ.IsArray() {
		if elementDecoderType, present := tm.typeMap.TypeForOID(typ.OidElement()); present {
			tm.typeMap.RegisterType(
				&pgtype.Type{
					Name:  typ.Name(),
					OID:   typ.Oid(),
					Codec: &pgtype.ArrayCodec{ElementType: elementDecoderType},
				},
			)
			return nil
		}
	}

	return errors.Errorf("Unknown codec for type registration with oid %d", typ.Oid())
}

func (tm *typeManager) resolveOptimizedTypeConverter(
	typ pgtypes.PgType, registration typeRegistration,
) pgtypes.TypeConverter {

	if registration.isArray {
		lazyConverter := &lazyArrayConverter{
			typeManager: tm,
			oidElement:  typ.OidElement(),
		}
		return lazyConverter.convert
	}
	return registration.converter
}
