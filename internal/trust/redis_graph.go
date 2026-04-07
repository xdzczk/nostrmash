package trust

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisClient interface {
	TxPipeline() redis.Pipeliner
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
}

type redisKeyspace struct {
	prefix string
}

func newRedisKeyspace(prefix string) redisKeyspace {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "nostrmash"
	}
	return redisKeyspace{prefix: prefix}
}

func (k redisKeyspace) runPrefix(runID int64, snapshotRef string) string {
	return fmt.Sprintf("%s:trust:run:%d:%s", k.prefix, runID, sanitizeSnapshotRef(snapshotRef))
}

func (k redisKeyspace) runNodesKey(runID int64, snapshotRef string) string {
	return k.runPrefix(runID, snapshotRef) + ":nodes"
}

func (k redisKeyspace) runAdjKey(runID int64, snapshotRef, pubkey string) string {
	return k.runPrefix(runID, snapshotRef) + ":adj:" + strings.TrimSpace(pubkey)
}

func (k redisKeyspace) runRevAdjKey(runID int64, snapshotRef, pubkey string) string {
	return k.runPrefix(runID, snapshotRef) + ":rev_adj:" + strings.TrimSpace(pubkey)
}

func (k redisKeyspace) runSeedsKey(runID int64, snapshotRef string) string {
	return k.runPrefix(runID, snapshotRef) + ":seeds"
}

func (k redisKeyspace) runMetaKey(runID int64, snapshotRef string) string {
	return k.runPrefix(runID, snapshotRef) + ":meta"
}

func (k redisKeyspace) activeSnapshotKey() string {
	return fmt.Sprintf("%s:trust:active_snapshot", k.prefix)
}

func sanitizeSnapshotRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "snapshot"
	}
	return strings.ReplaceAll(ref, " ", "_")
}

type redisSyncResult struct {
	SnapshotRef string
	EdgeCount   int64
	SeedCount   int64
	NodeCount   int64
}

func (r *Runtime) syncGraphToRedis(ctx context.Context, runID int64) (redisSyncResult, error) {
	if r.redis == nil {
		return redisSyncResult{}, fmt.Errorf("redis is not configured for trust runtime")
	}
	snapshotRef := fmt.Sprintf("sync-%d", time.Now().UTC().UnixNano())
	keys := newRedisKeyspace(r.redisKeyPrefix)
	ttl := 24 * time.Hour

	pipe := r.redis.TxPipeline()
	var edgeCount int64
	nodeSet := make(map[string]struct{})
	flushEvery := int64(1000)
	pending := int64(0)

	edgeRows, err := r.pool.Query(ctx, `
		SELECT follower_pubkey, followed_pubkey
		FROM follower_edges
	`)
	if err != nil {
		return redisSyncResult{}, fmt.Errorf("query follower edges for redis sync: %w", err)
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var follower string
		var followed string
		if err := edgeRows.Scan(&follower, &followed); err != nil {
			return redisSyncResult{}, fmt.Errorf("scan follower edge for redis sync: %w", err)
		}
		follower = strings.TrimSpace(follower)
		followed = strings.TrimSpace(followed)
		if follower == "" || followed == "" {
			continue
		}
		pipe.SAdd(ctx, keys.runAdjKey(runID, snapshotRef, follower), followed)
		pipe.SAdd(ctx, keys.runRevAdjKey(runID, snapshotRef, followed), follower)
		pipe.SAdd(ctx, keys.runNodesKey(runID, snapshotRef), follower, followed)
		nodeSet[follower] = struct{}{}
		nodeSet[followed] = struct{}{}
		edgeCount++
		pending += 3
		if pending >= flushEvery {
			if _, err := pipe.Exec(ctx); err != nil {
				return redisSyncResult{}, fmt.Errorf("flush redis graph edge sync pipeline: %w", err)
			}
			pending = 0
		}
	}
	if err := edgeRows.Err(); err != nil {
		return redisSyncResult{}, fmt.Errorf("read follower edge rows for redis sync: %w", err)
	}

	seeds, err := loadActiveSeeds(ctx, r.pool)
	if err != nil {
		return redisSyncResult{}, err
	}
	seedVals := make([]string, 0, len(seeds))
	for pubkey := range seeds {
		seedVals = append(seedVals, pubkey)
	}
	if len(seedVals) > 0 {
		pipe.SAdd(ctx, keys.runSeedsKey(runID, snapshotRef), stringSliceToAny(seedVals)...)
		pending++
	}

	pipe.HSet(ctx, keys.runMetaKey(runID, snapshotRef),
		"run_id", runID,
		"snapshot_ref", snapshotRef,
		"edge_count", edgeCount,
		"seed_count", len(seedVals),
		"synced_at", time.Now().UTC().Format(time.RFC3339Nano),
	)
	pipe.Expire(ctx, keys.runNodesKey(runID, snapshotRef), ttl)
	pipe.Expire(ctx, keys.runSeedsKey(runID, snapshotRef), ttl)
	pipe.Expire(ctx, keys.runMetaKey(runID, snapshotRef), ttl)
	for node := range nodeSet {
		pipe.Expire(ctx, keys.runAdjKey(runID, snapshotRef, node), ttl)
		pipe.Expire(ctx, keys.runRevAdjKey(runID, snapshotRef, node), ttl)
	}
	pending += 4
	if pending > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return redisSyncResult{}, fmt.Errorf("flush redis graph final pipeline: %w", err)
		}
	}

	if err := r.redis.Set(ctx, keys.activeSnapshotKey(), snapshotRef, ttl).Err(); err != nil {
		return redisSyncResult{}, fmt.Errorf("set active redis trust snapshot: %w", err)
	}

	return redisSyncResult{
		SnapshotRef: snapshotRef,
		EdgeCount:   edgeCount,
		SeedCount:   int64(len(seedVals)),
		NodeCount:   int64(len(nodeSet)),
	}, nil
}

func stringSliceToAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

type rankNode struct {
	Pubkey string
	Score  float64
}

func (r *Runtime) loadAdjacencyFromRedis(ctx context.Context, runID int64, snapshotRef string) (map[string][]string, map[string]struct{}, error) {
	if r.redis == nil {
		return nil, nil, fmt.Errorf("redis is not configured")
	}
	keys := newRedisKeyspace(r.redisKeyPrefix)
	nodes, err := r.redis.SMembers(ctx, keys.runNodesKey(runID, snapshotRef)).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("load redis trust nodes: %w", err)
	}
	adj := make(map[string][]string, len(nodes))
	nodeSet := make(map[string]struct{}, len(nodes))
	pipe := r.redis.TxPipeline()
	cmds := make(map[string]*redis.StringSliceCmd, len(nodes))
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		nodeSet[node] = struct{}{}
		cmds[node] = pipe.SMembers(ctx, keys.runAdjKey(runID, snapshotRef, node))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, nil, fmt.Errorf("load redis trust adjacency: %w", err)
	}
	for node, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil {
			return nil, nil, fmt.Errorf("load redis trust adjacency for %s: %w", node, err)
		}
		neighbors := make([]string, 0, len(vals))
		for _, v := range vals {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			neighbors = append(neighbors, v)
			nodeSet[v] = struct{}{}
		}
		adj[node] = neighbors
	}
	return adj, nodeSet, nil
}

func (r *Runtime) loadAdjacencyFromPostgres(ctx context.Context) (map[string][]string, map[string]struct{}, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT follower_pubkey, followed_pubkey
		FROM follower_edges
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("query follower edges for ranking: %w", err)
	}
	defer rows.Close()

	adj := make(map[string][]string)
	nodeSet := make(map[string]struct{})
	for rows.Next() {
		var follower string
		var followed string
		if err := rows.Scan(&follower, &followed); err != nil {
			return nil, nil, fmt.Errorf("scan follower edge for ranking: %w", err)
		}
		follower = strings.TrimSpace(follower)
		followed = strings.TrimSpace(followed)
		if follower == "" || followed == "" {
			continue
		}
		adj[follower] = append(adj[follower], followed)
		nodeSet[follower] = struct{}{}
		nodeSet[followed] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read follower edge rows for ranking: %w", err)
	}
	return adj, nodeSet, nil
}
