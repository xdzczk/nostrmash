package trust

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

// TestSyncGraphToRedis_RoundTrip exercises the trust graph Redis sync against a
// real Postgres and a real Redis: it seeds follower edges, syncs them into
// Redis, then reads the adjacency back and asserts it matches. It is gated on
// TEST_DATABASE_URL and TEST_REDIS_URL and skips when either is unset.
func TestSyncGraphToRedis_RoundTrip(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "trust-redis")
	redisURL := dbtest.RedisURL(t, "trust-redis")

	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "trustredis")
	if err := store.Migrate(ctx, pool, "trust-redis-test"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at)
		VALUES ('evt_seed', 'alice', 1, 3, 'sig', '', '{}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed source event: %v", err)
	}

	// (follower, followed): the sync records adjacency as follower -> followed.
	edges := [][2]string{
		{"alice", "bob"},
		{"alice", "carol"},
		{"bob", "carol"},
	}
	for _, e := range edges {
		if _, err := pool.Exec(ctx, `
			INSERT INTO follower_edges (followed_pubkey, follower_pubkey, source_event_id, contact_list_created_at, derivation_version)
			VALUES ($1, $2, 'evt_seed', 1, 1)
		`, e[1], e[0]); err != nil {
			t.Fatalf("seed follower edge %v: %v", e, err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO trust_seeds (pubkey, is_active) VALUES ('alice', true)`); err != nil {
		t.Fatalf("seed trust seed: %v", err)
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rc := redis.NewClient(opts)
	t.Cleanup(func() { _ = rc.Close() })

	prefix := fmt.Sprintf("nmtest:%d", time.Now().UnixNano())
	t.Cleanup(func() {
		iter := rc.Scan(context.Background(), 0, prefix+"*", 0).Iterator()
		var keys []string
		for iter.Next(context.Background()) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = rc.Del(context.Background(), keys...).Err()
		}
	})

	rt := NewRuntimeWithRedis(pool, rc, prefix, true, false)

	const runID = int64(1)
	result, err := rt.syncGraphToRedis(ctx, runID)
	if err != nil {
		t.Fatalf("sync graph to redis: %v", err)
	}
	if result.EdgeCount != int64(len(edges)) {
		t.Fatalf("edge count = %d, want %d", result.EdgeCount, len(edges))
	}
	if result.SeedCount != 1 {
		t.Fatalf("seed count = %d, want 1", result.SeedCount)
	}

	adj, nodeSet, err := rt.loadAdjacencyFromRedis(ctx, runID, result.SnapshotRef)
	if err != nil {
		t.Fatalf("load adjacency from redis: %v", err)
	}

	aliceNeighbors := append([]string(nil), adj["alice"]...)
	sort.Strings(aliceNeighbors)
	if len(aliceNeighbors) != 2 || aliceNeighbors[0] != "bob" || aliceNeighbors[1] != "carol" {
		t.Fatalf("alice adjacency = %v, want [bob carol]", aliceNeighbors)
	}
	if got := adj["bob"]; len(got) != 1 || got[0] != "carol" {
		t.Fatalf("bob adjacency = %v, want [carol]", got)
	}
	for _, node := range []string{"alice", "bob", "carol"} {
		if _, ok := nodeSet[node]; !ok {
			t.Fatalf("node set missing %q: %v", node, nodeSet)
		}
	}
}
