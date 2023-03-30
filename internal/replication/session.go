package replication

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"
)

type rowFunction = func(row pgx.Row) error

type session interface {
	exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)

	queryRow(ctx context.Context, query string, args ...any) pgx.Row

	queryFunc(ctx context.Context, fn rowFunction, query string, args ...any) error

	queryFuncWithTimeout(ctx context.Context, timeout time.Duration, fn rowFunction, query string, args ...any) error
}
