package pgtypes

import (
	"github.com/jackc/pglogrepl"
)

type TypeManager interface {
	ResolveDataType(
		oid uint32,
	) (PgType, error)
	ResolveTypeConverter(
		oid uint32,
	) (TypeConverter, error)
	NumKnownTypes() int
	DecodeTuples(
		relation *RelationMessage, tupleData *pglogrepl.TupleData,
	) (map[string]any, error)
	GetOrPlanTupleDecoder(relation *RelationMessage) (TupleDecoderPlan, error)
}
