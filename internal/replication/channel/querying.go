package channel

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"
)

type RowFunction = func(row pgx.Row) error

type QuerySession interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)

	QueryRow(ctx context.Context, query string, args ...any) pgx.Row

	QueryFunc(ctx context.Context, fn RowFunction, query string, args ...any) error

	QueryFuncWithTimeout(ctx context.Context, timeout time.Duration, fn RowFunction, query string, args ...any) error
}

type QueryAdapter interface {
	NewSession(fn func(session QuerySession) error) error
}
