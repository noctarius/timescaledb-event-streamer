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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/noctarius/timescaledb-event-streamer/internal/containers"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/samber/lo"
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

type typeRegistration struct {
	schemaType    schema.Type
	schemaBuilder schema.Builder
	isArray       bool
	oidElement    uint32
	converter     pgtypes.TypeConverter
	codec         pgtype.Codec
}

// errIllegalValue represents an illegal type conversion request
// for the given value
var errIllegalValue = fmt.Errorf("illegal value for data type conversion")

type typeManager struct {
	logger      *logging.Logger
	sideChannel sidechannel.SideChannel

	typeCache      map[uint32]pgtypes.PgType
	typeNameCache  map[string]uint32
	typeCacheMutex sync.RWMutex

	optimizedTypes      map[uint32]pgtypes.PgType
	optimizedConverters map[uint32]typeRegistration
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

		typeCache:      make(map[uint32]pgtypes.PgType),
		typeNameCache:  make(map[string]uint32),
		typeCacheMutex: sync.RWMutex{},

		optimizedTypes:      make(map[uint32]pgtypes.PgType),
		optimizedConverters: make(map[uint32]typeRegistration),
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

	// Extract keys from the built-in core types
	coreTypesSlice := lo.Keys(coreTypes)

	if err := tm.sideChannel.ReadPgTypes(tm.typeFactory, func(typ pgtypes.PgType) error {
		if lo.IndexOf(coreTypesSlice, typ.Oid()) != -1 {
			return nil
		}

		tm.typeCache[typ.Oid()] = typ
		tm.typeNameCache[typ.Name()] = typ.Oid()

		if registration, present := optimizedTypes[typ.Name()]; present {
			if t, ok := typ.(*pgType); ok {
				t.schemaType = registration.schemaType
			}

			tm.optimizedTypes[typ.Oid()] = typ

			var converter pgtypes.TypeConverter
			if registration.isArray {
				lazyConverter := &lazyArrayConverter{
					typeManager: tm,
					oidElement:  typ.OidElement(),
				}
				converter = lazyConverter.convert
			} else {
				converter = registration.converter
			}

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

			if typ.IsArray() {
				if elementType, present := pgtypes.GetType(typ.OidElement()); present {
					pgtypes.RegisterType(&pgtype.Type{
						Name: typ.Name(), OID: typ.Oid(), Codec: &pgtype.ArrayCodec{ElementType: elementType},
					})
				}
			} else {
				pgtypes.RegisterType(&pgtype.Type{Name: typ.Name(), OID: typ.Oid(), Codec: registration.codec})
			}
		}

		return nil
	}); err != nil {
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
		err := tm.sideChannel.ReadPgTypes(tm.typeFactory, func(p pgtypes.PgType) error {
			pt = p
			return nil
		}, oid)

		if err != nil {
			return false, err
		}

		if pt == nil {
			return false, nil
		}

		tm.typeCache[oid] = pt
		tm.typeNameCache[pt.Name()] = oid
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
		plan, err = pgtypes.PlanTupleDecoder(relation)
		if err != nil {
			return nil, err
		}
		tm.cachedDecoderPlans.Store(relation.RelationID, plan)
	}
	return plan, nil
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

type lazyArrayConverter struct {
	typeManager *typeManager
	oidElement  uint32
	converter   pgtypes.TypeConverter
}

func (lac *lazyArrayConverter) convert(
	oid uint32, value any,
) (any, error) {

	if lac.converter == nil {
		elementType, err := lac.typeManager.ResolveDataType(lac.oidElement)
		if err != nil {
			return nil, err
		}

		elementConverter, err := lac.typeManager.ResolveTypeConverter(lac.oidElement)
		if err != nil {
			return nil, err
		}

		reflectiveType, err := schemaType2ReflectiveType(elementType.SchemaType())
		if err != nil {
			return nil, err
		}

		targetType := reflect.SliceOf(reflectiveType)
		lac.converter = reflectiveArrayConverter(lac.oidElement, targetType, elementConverter)
	}

	return lac.converter(oid, value)
}

func schemaType2ReflectiveType(
	schemaType schema.Type,
) (reflect.Type, error) {

	switch schemaType {
	case schema.INT8:
		return int8Type, nil
	case schema.INT16:
		return int16Type, nil
	case schema.INT32:
		return int32Type, nil
	case schema.INT64:
		return int64Type, nil
	case schema.FLOAT32:
		return float32Type, nil
	case schema.FLOAT64:
		return float64Type, nil
	case schema.BOOLEAN:
		return booleanType, nil
	case schema.STRING:
		return stringType, nil
	case schema.BYTES:
		return byteaType, nil
	case schema.MAP:
		return mapType, nil
	default:
		return nil, errors.Errorf("Unsupported schema type %s", string(schemaType))
	}
}
