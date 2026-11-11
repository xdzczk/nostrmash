package appapi

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisStringCache stores a viewer's follow list under the trust Redis
// prefix. A cache miss or a Redis error falls through to Postgres.
type redisStringCache struct {
	client *redis.Client
	prefix string
}

func (c redisStringCache) Get(ctx context.Context, key string) (string, bool, error) {
	if c.client == nil {
		return "", false, nil
	}
	raw, err := c.client.Get(ctx, c.cacheKey(key)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return raw, true, nil
}

func (c redisStringCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	if c.client == nil {
		return nil
	}
	return c.client.Set(ctx, c.cacheKey(key), value, ttl).Err()
}

func (c redisStringCache) cacheKey(key string) string {
	prefix := strings.TrimSpace(c.prefix)
	if prefix == "" {
		prefix = "nostrmash"
	}
	return prefix + ":discovery:cohort:" + key
}
