package pgtypes

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
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
	GetOrPlanRowDecoder(fields []pgconn.FieldDescription) (RowDecoder, error)
}
