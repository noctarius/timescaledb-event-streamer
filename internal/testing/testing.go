package testing

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noctarius/event-stream-prototype/internal/supporting"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"strings"
	"time"
)

const (
	DatabaseSchema = "tsdb"
)

func CreateHypertable(pool *pgxpool.Pool, timeDimension string,
	chunkSize time.Duration, columns ...model.Column) (string, string, error) {

	tableName := randomTableName()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	columnDefinitions := make([]string, len(columns))
	for i, column := range columns {
		columnDefinitions[i] = toDefinition(column)
	}

	query := fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (%s)", DatabaseSchema,
		tableName, strings.Join(columnDefinitions, ", "))

	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	query = fmt.Sprintf(
		"SELECT create_hypertable('%s.%s', '%s', chunk_time_interval := interval '%d seconds')",
		DatabaseSchema, tableName, timeDimension, int64(chunkSize.Seconds()),
	)
	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	tx.Commit(context.Background())
	return DatabaseSchema, tableName, nil
}

func randomTableName() string {
	return supporting.RandomTextString(20)
}

func toDefinition(column model.Column) string {
	builder := strings.Builder{}
	builder.WriteString(column.Name())
	builder.WriteString(" ")
	builder.WriteString(column.TypeName())
	if column.Nullable() {
		builder.WriteString(" NULL")
	}
	if column.DefaultValue() != nil {
		builder.WriteString(fmt.Sprintf(" DEFAULT '%s'", *column.DefaultValue()))
	}
	if column.PrimaryKey() {
		builder.WriteString(" PRIMARY KEY")
	}
	return builder.String()
}
