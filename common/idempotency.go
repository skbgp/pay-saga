package common

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// IdempotencyStore handles API-level idempotency (Idempotency-Key header)
// and consumer-level dedup (Kafka at-least-once -> effectively-once).
type IdempotencyStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewIdempotencyStore creates a store backed by the given Redis address.
func NewIdempotencyStore(addr string, ttl time.Duration) *IdempotencyStore {
	return &IdempotencyStore{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		ttl:    ttl,
	}
}

type idempotencyEntry struct {
	BodyHash   string `json:"bh"`
	StatusCode int    `json:"sc"`
	Response   string `json:"rs"`
}

// HashBody returns the SHA-256 hex digest of the request body.
func HashBody(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

// Check looks up an idempotency key in Redis.
// Returns (entry, true, nil) on cache hit, (nil, false, nil) on miss.
func (s *IdempotencyStore) Check(ctx context.Context, key string) (*idempotencyEntry, bool, error) {
	rkey := "idem:" + key

	vals, err := s.client.HGetAll(ctx, rkey).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redis hgetall: %w", err)
	}

	if len(vals) == 0 {
		return nil, false, nil
	}

	sc := 0
	if v, ok := vals["sc"]; ok {
		fmt.Sscanf(v, "%d", &sc)
	}

	return &idempotencyEntry{
		BodyHash:   vals["bh"],
		StatusCode: sc,
		Response:   vals["rs"],
	}, true, nil
}

// Store saves an idempotency key with the response that was generated.
func (s *IdempotencyStore) Store(ctx context.Context, key string, bodyHash string, statusCode int, response string) error {
	rkey := "idem:" + key

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, rkey, map[string]interface{}{
		"bh": bodyHash,
		"sc": fmt.Sprintf("%d", statusCode),
		"rs": response,
	})
	pipe.Expire(ctx, rkey, s.ttl)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}
	return nil
}

// MarkSeen records a message ID as processed for consumer dedup.
// Returns true if already seen (duplicate), false if new.
func (s *IdempotencyStore) MarkSeen(ctx context.Context, topic string, messageID string) (bool, error) {
	rkey := "dedup:" + topic + ":" + messageID

	wasSet, err := s.client.SetNX(ctx, rkey, "1", s.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}

	return !wasSet, nil
}
