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

type typeFactory func(typeMap *pgtype.Map, typ pgtypes.PgType) *pgtype.Type

type codecFactory func(typeMap *pgtype.Map, typ pgtypes.PgType) pgtype.Codec

type converterFactory func(typeMap *pgtype.Map, typ pgtypes.PgType) pgtypes.TypeConverter

type typeRegistration struct {
	schemaType            schema.Type
	schemaBuilder         schema.Builder
	isArray               bool
	oidElement            uint32
	converter             pgtypes.TypeConverter
	codec                 pgtype.Codec
	converterFactory      converterFactory
	codecFactory          codecFactory
	typeFactory           typeFactory
	overrideExistingCodec bool
}

// errIllegalValue represents an illegal type conversion request
// for the given value
var errIllegalValue = fmt.Errorf("illegal value for data type conversion")

type typeManager struct {
	logger      *logging.Logger
	sideChannel sidechannel.SideChannel

	typeMap *pgtype.Map

	coreTypeCache           []pgtypes.PgType
	dynamicTypeCache        *containers.CasCache[uint32, pgtypes.PgType]
	optimizedConverterCache *containers.CasCache[uint32, typeRegistration]
	dynamicConverterCache   *containers.CasCache[uint32, typeRegistration]
	decoderPlanCache        *containers.CasCache[uint32, pgtypes.TupleDecoderPlan]
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

		coreTypeCache:           make([]pgtypes.PgType, upperCoreOidBound),
		dynamicTypeCache:        containers.NewCasCache[uint32, pgtypes.PgType](),
		optimizedConverterCache: containers.NewCasCache[uint32, typeRegistration](),
		dynamicConverterCache:   containers.NewCasCache[uint32, typeRegistration](),
		decoderPlanCache:        containers.NewCasCache[uint32, pgtypes.TupleDecoderPlan](),
	}

	if err := typeManager.initialize(); err != nil {
		return nil, err
	}
	return typeManager, nil
}

func (tm *typeManager) initialize() error {
	dynamicTypes := make(map[uint32]pgtypes.PgType)
	optimizedConverters := make(map[uint32]typeRegistration)
	dynamicConverters := make(map[uint32]typeRegistration)

	dynamicTypeSetter := func(oid uint32, typ pgtypes.PgType) {
		dynamicTypes[oid] = typ
	}
	optimizedConverterSetter := func(oid uint32, registration typeRegistration) {
		optimizedConverters[oid] = registration
	}
	dynamicConverterSetter := func(oid uint32, registration typeRegistration) {
		dynamicConverters[oid] = registration
	}

	registerTypeAdapter := func(typ pgtypes.PgType) error {
		return tm.registerType(typ, dynamicTypeSetter, dynamicConverterSetter, optimizedConverterSetter)
	}

	if err := tm.sideChannel.ReadPgTypes(tm.typeFactory, registerTypeAdapter); err != nil {
		return err
	}

	tm.dynamicTypeCache.SetAll(dynamicTypes)
	tm.dynamicConverterCache.SetAll(dynamicConverters)
	tm.optimizedConverterCache.SetAll(optimizedConverters)
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

	if oid < upperCoreOidBound {
		if typ := tm.coreTypeCache[oid]; typ != nil {
			return typ, nil
		}
	}

	return tm.dynamicTypeCache.GetOrCompute(oid, func() (pgtypes.PgType, error) {
		var pt pgtypes.PgType
		err := tm.sideChannel.ReadPgTypes(tm.typeFactory, func(typ pgtypes.PgType) error {
			pt = typ
			return tm.registerType(
				typ, tm.dynamicTypeCache.Set, tm.dynamicConverterCache.Set, tm.optimizedConverterCache.Set,
			)
		}, oid)
		return pt, err
	})
}

func (tm *typeManager) ResolveTypeConverter(
	oid uint32,
) (pgtypes.TypeConverter, error) {

	if registration, present := coreType(oid); present {
		return registration.converter, nil
	}
	if registration, present := tm.optimizedConverterCache.Get(oid); present {
		if registration.converterFactory != nil {
			typ, err := tm.ResolveDataType(oid)
			if err != nil {
				return nil, err
			}
			return registration.converterFactory(tm.typeMap, typ), nil
		}
		return registration.converter, nil
	}
	if registration, present := tm.dynamicConverterCache.Get(oid); present {
		return registration.converter, nil
	}
	return nil, fmt.Errorf("unsupported OID: %d", oid)
}

func (tm *typeManager) NumKnownTypes() int {
	return tm.dynamicTypeCache.Length()
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

	return tm.decoderPlanCache.GetOrCompute(
		relation.RelationID,
		func() (pgtypes.TupleDecoderPlan, error) {
			return planTupleDecoder(tm, relation)
		},
	)
}

func (tm *typeManager) GetOrPlanRowDecoder(
	fields []pgconn.FieldDescription,
) (pgtypes.RowDecoder, error) {

	return newRowDecoder(tm, fields)
}

func (tm *typeManager) RegisterColumnType(
	column schema.ColumnAlike,
) error {

	return tm.lazilyRegisterTypeMap(column.DataType())
}

func (tm *typeManager) lazilyRegisterTypeMap(
	oid uint32,
) error {

	if !tm.knownInTypeMap(oid) {
		typ, err := tm.ResolveDataType(oid)
		if err != nil {
			return err
		}

		if typ.IsArray() {
			elementType := typ.ElementType()
			if err := tm.lazilyRegisterTypeMap(elementType.Oid()); err != nil {
				return err
			}

			if elementType.Kind() == pgtypes.EnumKind {
				if pt, present := tm.typeMap.TypeForOID(elementType.Oid()); present {
					registration := typeRegistration{
						schemaType: elementType.SchemaType(),
						converter:  arrayConverter[[]string](elementType.Oid(), enum2string),
						codec:      &pgtype.ArrayCodec{ElementType: pt},
					}
					tm.dynamicConverterCache.Set(typ.Oid(), registration)
					if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
						return err
					}
				}

			} else if elementType.SchemaType() == schema.STRUCT {
				converter, err := newCompositeConverter(tm, elementType)
				if err != nil {
					return err
				}

				reflectiveType, err := schemaType2ReflectiveType(elementType.SchemaType())
				if err != nil {
					return err
				}

				targetType := reflect.SliceOf(reflectiveType)
				registration := typeRegistration{
					schemaType:    schema.ARRAY,
					schemaBuilder: tm.resolveSchemaBuilder(typ),
					isArray:       true,
					oidElement:    typ.OidElement(),
					converter:     reflectiveArrayConverter(elementType.Oid(), targetType, converter),
				}

				tm.dynamicConverterCache.Set(typ.Oid(), registration)
				if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
					return err
				}
			}

			// We only handle struct schema types here. Everything else should
			// already be registered, or this is a bug
		} else if typ.SchemaType() == schema.STRUCT {
			codec, err := newCompositeCodec(tm, typ)
			if err != nil {
				return err
			}

			converter, err := newCompositeConverter(tm, typ)
			if err != nil {
				return err
			}

			registration, err := tm.dynamicConverterCache.TransformSetAndGet(
				typ.Oid(), func(old typeRegistration) (typeRegistration, error) {
					old.codec = codec
					old.converter = converter
					return old, nil
				},
			)
			if err != nil {
				return err
			}

			if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
				return err
			}
		} else {
			return errors.Errorf("Cannot lazily register type %d in type map", oid)
		}
	}
	return nil
}

func (tm *typeManager) findType(
	oid uint32,
) (typ pgtypes.PgType, present bool) {

	if oid < upperCoreOidBound {
		if typ = tm.coreTypeCache[oid]; typ != nil {
			return typ, true
		}
	}
	typ, present = tm.dynamicTypeCache.Get(oid)
	return
}

func (tm *typeManager) getSchemaType(
	oid uint32, arrayType bool, kind pgtypes.PgKind,
) schema.Type {

	if registration, present := coreType(oid); present {
		return registration.schemaType
	}
	if registration, present := tm.optimizedConverterCache.Get(oid); present {
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
	typ pgtypes.PgType,
) schema.Builder {

	if registration, present := tm.optimizedConverterCache.Get(typ.Oid()); present {
		if registration.schemaBuilder != nil {
			return registration.schemaBuilder
		}
	}

	switch typ.SchemaType() {
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
		if typ.Kind() == pgtypes.EnumKind {
			return schema.Enum(typ.EnumValues())
		}
		switch typ.Oid() {
		case pgtype.JSONOID, pgtype.JSONBOID:
			return schema.Json()

		case pgtype.UUIDOID:
			return schema.Uuid()

		case pgtype.BitOID:
			// TODO: needs better handling

		case pgtypes.XmlOID: // XML
			return schema.Xml()
		}
		return schema.String()

	case schema.BYTES:
		return schema.Bytes()

	case schema.ARRAY:
		elementType := typ.ElementType()
		return schema.NewSchemaBuilder(typ.SchemaType()).ValueSchema(elementType.SchemaBuilder())

	case schema.MAP:
		// FIXME: Implement Map Schema Type
		return nil

	case schema.STRUCT:
		if typ.Kind() == pgtypes.CompositeKind {
			columns, err := typ.CompositeColumns()
			if err != nil {
				panic(err)
			}
			schemaBuilder := schema.NewSchemaBuilder(typ.SchemaType())
			for i, column := range columns {
				schemaBuilder = schemaBuilder.Field(column.Name(), i, column.SchemaBuilder())
			}
			return schemaBuilder.Clone()
		}

		if typ.Kind() == pgtypes.DomainKind {
			baseType := typ.BaseType()
			return baseType.SchemaBuilder().SchemaName(fmt.Sprintf("%s.Type", typ.Name())).Clone()
		}

		return nil

	default:
		return nil
	}
}

func (tm *typeManager) resolveCompositeTypeColumns(
	typ *pgType,
) ([]pgtypes.CompositeColumn, error) {

	return tm.sideChannel.ReadPgCompositeTypeSchema(
		typ.oid, func(name string, oid uint32, modifiers int, nullable bool) (pgtypes.CompositeColumn, error) {
			columnType, present := tm.findType(oid)
			if !present {
				return nil, errors.Errorf("Type with oid %d not found", oid)
			}
			return newCompositeColumn(name, columnType, modifiers, nullable), nil
		},
	)
}

func (tm *typeManager) registerType(
	typ pgtypes.PgType, dynamicTypeSetter func(uint32, pgtypes.PgType),
	dynamicConverterSetter func(uint32, typeRegistration),
	optimizedConverterSetter func(uint32, typeRegistration),
) error {

	// Make sure we store the type for optimized core types
	if _, present := coreType(typ.Oid()); present {
		tm.coreTypeCache[typ.Oid()] = typ
	}

	// And store it like anything else
	dynamicTypeSetter(typ.Oid(), typ)

	// Is core type not available in TypeMap by default (bug or not implemented in pgx)?
	if registration, present := coreType(typ.Oid()); present {
		if !tm.knownInTypeMap(typ.Oid()) || registration.overrideExistingCodec {
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

		converter := tm.resolveOptimizedTypeConverter(typ, registration)
		if converter == nil {
			return errors.Errorf("Type %s has no assigned value converter", typ.Name())
		}

		optimizedConverterSetter(typ.Oid(), typeRegistration{
			schemaType:       registration.schemaType,
			schemaBuilder:    registration.schemaBuilder,
			isArray:          registration.isArray,
			oidElement:       typ.OidElement(),
			converter:        converter,
			converterFactory: registration.converterFactory,
			codec:            registration.codec,
			codecFactory:     registration.codecFactory,
		})

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

		dynamicConverterSetter(typ.Oid(), registration)
		if err := tm.registerTypeInTypeMap(typ, registration); err != nil {
			return err
		}
	}

	// Object types (all remaining) need to be handled specifically
	if typ.SchemaType() == schema.STRUCT {
		if typ.Kind() == pgtypes.CompositeKind {
			registration := typeRegistration{
				schemaType: typ.SchemaType(),
			}
			dynamicConverterSetter(typ.Oid(), registration)
			// Attention: Type map registration is lazy to prevent heavy
			// reading of columns for unused types:
			// See @RegisterColumnTypes
		}

		// TODO: ignore all other types for now - missing implementation
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
	if registration.codec != nil || registration.codecFactory != nil {
		codec := registration.codec
		if registration.codecFactory != nil {
			codec = registration.codecFactory(tm.typeMap, typ)
		}

		if codec == nil {
			return errors.Errorf("No valid codec provided for type %s", typ.Name())
		}

		tm.typeMap.RegisterType(&pgtype.Type{Name: typ.Name(), OID: typ.Oid(), Codec: codec})
		return nil
	}

	// Slightly more complicated types have a factory for the pgx type
	if registration.typeFactory != nil {
		tm.typeMap.RegisterType(registration.typeFactory(tm.typeMap, typ))
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
	if registration.converterFactory != nil {
		return registration.converterFactory(tm.typeMap, typ)
	}
	return registration.converter
}
