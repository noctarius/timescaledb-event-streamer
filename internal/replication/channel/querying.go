package channel

import (
	"context"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
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

	AttachChunkToPublication(chunk *model.Chunk) error

	DetachChunkFromPublication(chunk *model.Chunk) error

	SnapshotTable(canonicalName string, startingLSN *pglogrepl.LSN,
		cb func(lsn pglogrepl.LSN, values map[string]any) error) (pglogrepl.LSN, error)
}
