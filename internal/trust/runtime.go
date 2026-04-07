package trust

import (
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Runtime struct {
	pool               *pgxpool.Pool
	redis              redisClient
	redisKeyPrefix     string
	enableRedisSync    bool
	enableScoreCompute bool
}

func NewRuntime(pool *pgxpool.Pool, enableRedisSync, enableScoreCompute bool) *Runtime {
	return NewRuntimeWithRedis(pool, nil, "nostrmash", enableRedisSync, enableScoreCompute)
}

func NewRuntimeWithRedis(
	pool *pgxpool.Pool,
	redis redisClient,
	redisKeyPrefix string,
	enableRedisSync, enableScoreCompute bool,
) *Runtime {
	return &Runtime{
		pool:               pool,
		redis:              redis,
		redisKeyPrefix:     strings.TrimSpace(redisKeyPrefix),
		enableRedisSync:    enableRedisSync,
		enableScoreCompute: enableScoreCompute,
	}
}
