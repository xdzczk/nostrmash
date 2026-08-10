package trust

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	defaultPersonalizedMaxSeedFollows = 2000
	defaultPersonalizedCacheTTL       = time.Hour
	personalizedSourcePersonalized    = "personalized"
	personalizedSourceGlobalFallback  = "global_fallback"
)

// PersonalizedScore is one row of a viewer-scoped trust ranking.
type PersonalizedScore struct {
	Pubkey string  `json:"pubkey"`
	Score  float64 `json:"score"`
	Rank   int64   `json:"rank"`
	RunID  int64   `json:"run_id"`
	Source string  `json:"source"`
}

type personalizedCache interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
}

// PersonalizedRanker computes viewer-scoped trust rankings from the follow
// graph. Results are optionally cached in Redis keyed by active trust run id.
type PersonalizedRanker struct {
	pool           *pgxpool.Pool
	cache          personalizedCache
	redisKeyPrefix string
	maxSeedFollows int
	cacheTTL       time.Duration
}

func NewPersonalizedRanker(pool *pgxpool.Pool, maxSeedFollows int) *PersonalizedRanker {
	if maxSeedFollows <= 0 {
		maxSeedFollows = defaultPersonalizedMaxSeedFollows
	}
	return &PersonalizedRanker{
		pool:           pool,
		redisKeyPrefix: "nostrmash",
		maxSeedFollows: maxSeedFollows,
		cacheTTL:       defaultPersonalizedCacheTTL,
	}
}

func (r *PersonalizedRanker) WithRedis(cache personalizedCache, keyPrefix string) *PersonalizedRanker {
	if r == nil {
		return nil
	}
	r.cache = cache
	if strings.TrimSpace(keyPrefix) != "" {
		r.redisKeyPrefix = strings.TrimSpace(keyPrefix)
	}
	return r
}

func (r *PersonalizedRanker) GetRanking(ctx context.Context, viewerPubkey string, limit int) ([]PersonalizedScore, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("personalized ranker is not initialized")
	}
	viewerPubkey = strings.TrimSpace(viewerPubkey)
	if viewerPubkey == "" {
		return nil, fmt.Errorf("viewer pubkey is required")
	}
	if limit <= 0 {
		limit = 50
	}

	runID, err := latestSucceededTrustRunID(ctx, r.pool)
	if err != nil {
		return nil, err
	}

	if cached, ok, err := r.readCache(ctx, runID, viewerPubkey); err != nil {
		return nil, err
	} else if ok {
		return truncatePersonalizedScores(cached, limit), nil
	}

	follows, err := listFollowedPubkeys(ctx, r.pool, viewerPubkey, r.maxSeedFollows+1)
	if err != nil {
		return nil, err
	}
	if len(follows) == 0 || len(follows) > r.maxSeedFollows {
		scores, err := r.globalFallback(ctx, runID, limit)
		if err != nil {
			return nil, err
		}
		_ = r.writeCache(ctx, runID, viewerPubkey, scores)
		return scores, nil
	}

	adjacency, nodeSet, err := LoadAdjacencyFromPostgres(ctx, r.pool)
	if err != nil {
		return nil, err
	}
	if len(nodeSet) == 0 {
		scores, err := r.globalFallback(ctx, runID, limit)
		if err != nil {
			return nil, err
		}
		_ = r.writeCache(ctx, runID, viewerPubkey, scores)
		return scores, nil
	}

	teleport := teleportFromFollows(follows)
	// Always include the viewer so teleport mass is not empty when follows are
	// outside the loaded graph.
	teleport[viewerPubkey] = 1
	ranked := ComputePersonalizedRank(adjacency, nodeSet, teleport, rankDamping)
	scores := make([]PersonalizedScore, 0, min(limit, len(ranked)))
	for i, node := range ranked {
		if i >= limit {
			break
		}
		scores = append(scores, PersonalizedScore{
			Pubkey: node.Pubkey,
			Score:  node.Score,
			Rank:   int64(i + 1),
			RunID:  runID,
			Source: personalizedSourcePersonalized,
		})
	}
	_ = r.writeCache(ctx, runID, viewerPubkey, scores)
	return scores, nil
}

func (r *PersonalizedRanker) globalFallback(ctx context.Context, runID int64, limit int) ([]PersonalizedScore, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT pubkey, score, rank
		FROM trust_scores_global
		ORDER BY rank ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list global trust scores for personalized fallback: %w", err)
	}
	defer rows.Close()

	out := make([]PersonalizedScore, 0, limit)
	for rows.Next() {
		var item PersonalizedScore
		if err := rows.Scan(&item.Pubkey, &item.Score, &item.Rank); err != nil {
			return nil, fmt.Errorf("scan global trust score fallback: %w", err)
		}
		item.RunID = runID
		item.Source = personalizedSourceGlobalFallback
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read global trust score fallback: %w", err)
	}
	return out, nil
}

func (r *PersonalizedRanker) cacheKey(runID int64, viewerPubkey string) string {
	prefix := strings.TrimSpace(r.redisKeyPrefix)
	if prefix == "" {
		prefix = "nostrmash"
	}
	return fmt.Sprintf("%s:trust:personalized:%d:%s", prefix, runID, viewerPubkey)
}

func (r *PersonalizedRanker) readCache(ctx context.Context, runID int64, viewerPubkey string) ([]PersonalizedScore, bool, error) {
	if r.cache == nil || runID <= 0 {
		return nil, false, nil
	}
	raw, err := r.cache.Get(ctx, r.cacheKey(runID, viewerPubkey)).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read personalized trust cache: %w", err)
	}
	var scores []PersonalizedScore
	if err := json.Unmarshal([]byte(raw), &scores); err != nil {
		return nil, false, nil
	}
	return scores, true, nil
}

func (r *PersonalizedRanker) writeCache(ctx context.Context, runID int64, viewerPubkey string, scores []PersonalizedScore) error {
	if r.cache == nil || runID <= 0 || len(scores) == 0 {
		return nil
	}
	ttl := r.cacheTTL
	if ttl <= 0 {
		ttl = defaultPersonalizedCacheTTL
	}
	payload, err := json.Marshal(scores)
	if err != nil {
		return fmt.Errorf("encode personalized trust cache: %w", err)
	}
	if err := r.cache.Set(ctx, r.cacheKey(runID, viewerPubkey), payload, ttl).Err(); err != nil {
		return fmt.Errorf("write personalized trust cache: %w", err)
	}
	return nil
}

func teleportFromFollows(follows []string) map[string]float64 {
	out := make(map[string]float64, len(follows))
	for _, follow := range follows {
		follow = strings.TrimSpace(follow)
		if follow == "" {
			continue
		}
		out[follow] = 1
	}
	return out
}

func listFollowedPubkeys(ctx context.Context, pool *pgxpool.Pool, followerPubkey string, limit int) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT followed_pubkey
		FROM follower_edges
		WHERE follower_pubkey = $1
		ORDER BY followed_pubkey ASC
		LIMIT $2
	`, followerPubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("list followed pubkeys: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var followed string
		if err := rows.Scan(&followed); err != nil {
			return nil, fmt.Errorf("scan followed pubkey: %w", err)
		}
		followed = strings.TrimSpace(followed)
		if followed == "" {
			continue
		}
		out = append(out, followed)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read followed pubkeys: %w", err)
	}
	return out, nil
}

func latestSucceededTrustRunID(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var runID int64
	err := pool.QueryRow(ctx, `
		SELECT id
		FROM trust_runs
		WHERE status = $1
		ORDER BY COALESCE(finished_at, updated_at) DESC, id DESC
		LIMIT 1
	`, RunStatusSucceeded).Scan(&runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("load latest succeeded trust run: %w", err)
	}
	return runID, nil
}

func truncatePersonalizedScores(scores []PersonalizedScore, limit int) []PersonalizedScore {
	if limit <= 0 || len(scores) <= limit {
		return scores
	}
	return append([]PersonalizedScore(nil), scores[:limit]...)
}
