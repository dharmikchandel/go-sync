package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Locker struct {
	client *redis.Client
}

func New(addr string) *Locker {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &Locker{client: rdb}
}

func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return l.client.SetNX(ctx, key, "locked", ttl).Result()
}

func (l *Locker) Release(ctx context.Context, key string) error {
	return l.client.Del(ctx, key).Err()
}