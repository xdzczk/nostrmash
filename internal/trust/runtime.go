package trust

import (
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Runtime struct {
	pool                   *pgxpool.Pool
	redis                  redisClient
	redisKeyPrefix         string
	enableRedisSync        bool
	enableScoreCompute     bool
	enableNeighborhoods     bool
	neighborhoodMaxMembers  int
	neighborhoodMaxHops     int
	enableInteractionGraph  bool
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
		pool:                   pool,
		redis:                  redis,
		redisKeyPrefix:         strings.TrimSpace(redisKeyPrefix),
		enableRedisSync:        enableRedisSync,
		enableScoreCompute:     enableScoreCompute,
		enableNeighborhoods:    false,
		neighborhoodMaxMembers: 5000,
		neighborhoodMaxHops:    3,
	}
}

// WithNeighborhoods configures optional seeded-neighborhood computation.
// maxMembers/maxHops <= 0 keep the runtime defaults (5000 / 3).
func (r *Runtime) WithNeighborhoods(enabled bool, maxMembers, maxHops int) *Runtime {
	if r == nil {
		return nil
	}
	r.enableNeighborhoods = enabled
	if maxMembers > 0 {
		r.neighborhoodMaxMembers = maxMembers
	}
	if maxHops > 0 {
		r.neighborhoodMaxHops = maxHops
	}
	return r
}

// WithInteractionGraph enables merging engagement-derived edge weights into
// ranking adjacency. Default off keeps global rank byte-compatible with the
// follow-only graph.
func (r *Runtime) WithInteractionGraph(enabled bool) *Runtime {
	if r == nil {
		return nil
	}
	r.enableInteractionGraph = enabled
	return r
}
