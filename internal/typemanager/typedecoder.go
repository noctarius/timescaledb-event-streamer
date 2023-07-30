package typemanager

import (
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type typeDecoder func(src []byte) (any, error)

func findTypeDecoder(
	typeManager *typeManager, field pgconn.FieldDescription,
) (typeDecoder, error) {

	if pgType, ok := typeManager.typeMap.TypeForOID(field.DataTypeOID); ok {
		// Store a decoder wrapper for easier usage
		return asTypeDecoder(typeManager, pgType, field), nil
	}
	return nil, errors.Errorf("Unsupported type oid: %d", field.DataTypeOID)
}

func asTypeDecoder(
	typeManager *typeManager, pgType *pgtype.Type, field pgconn.FieldDescription,
) func(src []byte) (any, error) {

	return func(src []byte) (any, error) {
		return pgType.Codec.DecodeValue(typeManager.typeMap, field.DataTypeOID, field.Format, src)
	}
}
