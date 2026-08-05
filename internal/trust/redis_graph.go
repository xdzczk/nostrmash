package trust

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type redisClient interface {
	TxPipeline() redis.Pipeliner
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	Unlink(ctx context.Context, keys ...string) *redis.IntCmd
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

// runScanPattern matches every per-run key across all runs and snapshot refs,
// used by the stale-run reaper.
func (k redisKeyspace) runScanPattern() string {
	return fmt.Sprintf("%s:trust:run:*", k.prefix)
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

// redisRunKeyTTL bounds the lifetime of every per-run key so that even if
// cleanup and reaping both fail, no key outlives the run by more than a day.
const redisRunKeyTTL = 24 * time.Hour

// redisCleanupTimeout bounds the best-effort SCAN+UNLINK passes that run
// outside the job's own context (which may already be cancelled on failure).
const redisCleanupTimeout = 5 * time.Minute

func (r *Runtime) syncGraphToRedis(ctx context.Context, runID int64) (redisSyncResult, error) {
	if r.redis == nil {
		return redisSyncResult{}, fmt.Errorf("redis is not configured for trust runtime")
	}
	snapshotRef := fmt.Sprintf("sync-%d", time.Now().UTC().UnixNano())
	keys := newRedisKeyspace(r.redisKeyPrefix)

	result, err := r.writeGraphSnapshot(ctx, runID, snapshotRef, keys)
	if err != nil {
		// Best-effort: delete whatever this attempt managed to write so a
		// retry (which uses a fresh snapshotRef) does not stack another full
		// keyset on top of the partial one. Every key already carries a TTL,
		// so a failed cleanup only delays reclamation, it cannot leak.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), redisCleanupTimeout)
		_ = r.unlinkMatching(cleanupCtx, keys.runPrefix(runID, snapshotRef)+":*", "")
		cancel()
		return redisSyncResult{}, err
	}

	if err := r.redis.Set(ctx, keys.activeSnapshotKey(), snapshotRef, redisRunKeyTTL).Err(); err != nil {
		return redisSyncResult{}, fmt.Errorf("set active redis trust snapshot: %w", err)
	}

	// Best-effort reap of keys from earlier runs and failed attempts. The run
	// scheduler guarantees a single active run, so once this sync has become
	// the active snapshot every other trust:run:* keyset is dead weight.
	// This also removes historical TTL-less keys leaked by older versions.
	reapCtx, cancel := context.WithTimeout(context.Background(), redisCleanupTimeout)
	_ = r.unlinkMatching(reapCtx, keys.runScanPattern(), keys.runPrefix(runID, snapshotRef)+":")
	cancel()

	return result, nil
}

// writeGraphSnapshot streams follower edges into per-run Redis keys. Every key
// receives its TTL in the same MULTI/EXEC batch as its first write, so a key
// can only exist with an expiry attached: a partially failed sync can never
// leave immortal keys behind (the pre-existing failure mode that grew the
// production keyspace unboundedly).
func (r *Runtime) writeGraphSnapshot(ctx context.Context, runID int64, snapshotRef string, keys redisKeyspace) (redisSyncResult, error) {
	pipe := r.redis.TxPipeline()
	var edgeCount int64
	nodeSet := make(map[string]struct{})
	adjTouched := make(map[string]struct{})
	revTouched := make(map[string]struct{})
	nodesKeyTouched := false
	flushEvery := int64(1000)
	pending := int64(0)

	if err := withHeavyStatementTimeout(ctx, r.pool, trustEdgeScanStatementTimeout, func(conn *pgxpool.Conn) error {
		edgeRows, err := conn.Query(ctx, `
			SELECT follower_pubkey, followed_pubkey
			FROM follower_edges
		`)
		if err != nil {
			return fmt.Errorf("query follower edges for redis sync: %w", err)
		}
		defer edgeRows.Close()
		for edgeRows.Next() {
			var follower string
			var followed string
			if err := edgeRows.Scan(&follower, &followed); err != nil {
				return fmt.Errorf("scan follower edge for redis sync: %w", err)
			}
			follower = strings.TrimSpace(follower)
			followed = strings.TrimSpace(followed)
			if follower == "" || followed == "" {
				continue
			}

			adjKey := keys.runAdjKey(runID, snapshotRef, follower)
			pipe.SAdd(ctx, adjKey, followed)
			pending++
			if _, ok := adjTouched[follower]; !ok {
				adjTouched[follower] = struct{}{}
				pipe.Expire(ctx, adjKey, redisRunKeyTTL)
				pending++
			}

			revKey := keys.runRevAdjKey(runID, snapshotRef, followed)
			pipe.SAdd(ctx, revKey, follower)
			pending++
			if _, ok := revTouched[followed]; !ok {
				revTouched[followed] = struct{}{}
				pipe.Expire(ctx, revKey, redisRunKeyTTL)
				pending++
			}

			nodesKey := keys.runNodesKey(runID, snapshotRef)
			pipe.SAdd(ctx, nodesKey, follower, followed)
			pending++
			if !nodesKeyTouched {
				nodesKeyTouched = true
				pipe.Expire(ctx, nodesKey, redisRunKeyTTL)
				pending++
			}

			nodeSet[follower] = struct{}{}
			nodeSet[followed] = struct{}{}
			edgeCount++
			if pending >= flushEvery {
				if err := execRedisPipe(ctx, pipe, "flush redis graph edge sync pipeline"); err != nil {
					return err
				}
				pending = 0
			}
		}
		if err := edgeRows.Err(); err != nil {
			return fmt.Errorf("read follower edge rows for redis sync: %w", err)
		}
		return nil
	}); err != nil {
		return redisSyncResult{}, err
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
		seedsKey := keys.runSeedsKey(runID, snapshotRef)
		pipe.SAdd(ctx, seedsKey, stringSliceToAny(seedVals)...)
		pipe.Expire(ctx, seedsKey, redisRunKeyTTL)
		pending += 2
	}

	metaKey := keys.runMetaKey(runID, snapshotRef)
	pipe.HSet(ctx, metaKey,
		"run_id", runID,
		"snapshot_ref", snapshotRef,
		"edge_count", edgeCount,
		"seed_count", len(seedVals),
		"synced_at", time.Now().UTC().Format(time.RFC3339Nano),
	)
	pipe.Expire(ctx, metaKey, redisRunKeyTTL)
	pending += 2
	if pending > 0 {
		if err := execRedisPipe(ctx, pipe, "flush redis graph final pipeline"); err != nil {
			return redisSyncResult{}, err
		}
	}

	return redisSyncResult{
		SnapshotRef: snapshotRef,
		EdgeCount:   edgeCount,
		SeedCount:   int64(len(seedVals)),
		NodeCount:   int64(len(nodeSet)),
	}, nil
}

// execRedisPipe executes a pipeline batch and, on failure, surfaces the first
// per-command error. With MULTI/EXEC the top-level error is often a bare
// EXECABORT that hides which command actually failed, which made the
// production sync failures undiagnosable from logs.
func execRedisPipe(ctx context.Context, pipe redis.Pipeliner, stage string) error {
	cmds, err := pipe.Exec(ctx)
	if err == nil {
		return nil
	}
	for _, cmd := range cmds {
		if cmdErr := cmd.Err(); cmdErr != nil && cmdErr != redis.Nil && cmdErr.Error() != err.Error() {
			return fmt.Errorf("%s: %w (first failing command %s: %v)", stage, err, cmd.Name(), cmdErr)
		}
	}
	return fmt.Errorf("%s: %w", stage, err)
}

// unlinkMatching scans keys matching pattern and UNLINKs them in batches,
// skipping keys that start with keepPrefix (pass "" to delete every match).
// SCAN is cursor-based and UNLINK reclaims memory asynchronously, so this is
// safe to run against a large production keyspace.
func (r *Runtime) unlinkMatching(ctx context.Context, pattern string, keepPrefix string) error {
	const unlinkBatch = 512
	var cursor uint64
	batch := make([]string, 0, unlinkBatch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := r.redis.Unlink(ctx, batch...).Err(); err != nil {
			return fmt.Errorf("unlink stale trust run keys: %w", err)
		}
		batch = batch[:0]
		return nil
	}
	for {
		found, next, err := r.redis.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return fmt.Errorf("scan trust run keys: %w", err)
		}
		for _, key := range found {
			if keepPrefix != "" && strings.HasPrefix(key, keepPrefix) {
				continue
			}
			batch = append(batch, key)
			if len(batch) >= unlinkBatch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return flush()
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
	adj := make(map[string][]string)
	nodeSet := make(map[string]struct{})
	if err := withHeavyStatementTimeout(ctx, r.pool, trustEdgeScanStatementTimeout, func(conn *pgxpool.Conn) error {
		rows, err := conn.Query(ctx, `
			SELECT follower_pubkey, followed_pubkey
			FROM follower_edges
		`)
		if err != nil {
			return fmt.Errorf("query follower edges for ranking: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var follower string
			var followed string
			if err := rows.Scan(&follower, &followed); err != nil {
				return fmt.Errorf("scan follower edge for ranking: %w", err)
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
			return fmt.Errorf("read follower edge rows for ranking: %w", err)
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return adj, nodeSet, nil
}
