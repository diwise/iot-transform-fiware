package rules

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadSnapshot reuses the normal MVCC rule reader on a caller-owned pool.
// It performs no migrations or mutations and does not take pool ownership.
func ReadSnapshot(ctx context.Context, pool *pgxpool.Pool) ([]StoredRule, int64, error) {
	return (&PgStore{pool: pool}).LoadRules(ctx)
}
