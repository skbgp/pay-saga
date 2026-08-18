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

// Consumer dedup states. A message is claimed before the work starts and only
// marked done once the work succeeded.
const (
	dedupProcessing = "processing"
	dedupDone       = "done"

	// claimTTL bounds how long a crashed consumer can block redelivery of a
	// message it claimed but never finished. After it lapses the message is
	// eligible to be processed again.
	claimTTL = 5 * time.Minute
)

// ClaimMessage attempts to take ownership of a Kafka message before doing any
// work with it. It reports whether the caller should process the message.
//
// The previous version marked a message as seen and then did the work, so a
// crash in between meant the redelivery was skipped as a "duplicate" and the
// work never happened at all — at-most-once delivery, in a system that needs
// at-least-once. Claim first, confirm after, and a crash results in a retry
// rather than a silently dropped order.
func (s *IdempotencyStore) ClaimMessage(ctx context.Context, topic string, messageID string) (bool, error) {
	rkey := "dedup:" + topic + ":" + messageID

	claimed, err := s.client.SetNX(ctx, rkey, dedupProcessing, claimTTL).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}
	if claimed {
		return true, nil
	}

	// Someone already holds this key. If they finished, it's a real duplicate.
	// If they're still working (or died mid-flight), skip it for now — the
	// claim expires and Kafka redelivers.
	state, err := s.client.Get(ctx, rkey).Result()
	if err != nil {
		if err == redis.Nil {
			// Claim expired between our SetNX and Get; let the redelivery handle it.
			return false, nil
		}
		return false, fmt.Errorf("redis get: %w", err)
	}

	if state == dedupDone {
		return false, nil
	}
	return false, nil
}

// ConfirmProcessed marks a claimed message as fully handled, so redeliveries
// are skipped from here on.
func (s *IdempotencyStore) ConfirmProcessed(ctx context.Context, topic string, messageID string) error {
	rkey := "dedup:" + topic + ":" + messageID

	if err := s.client.Set(ctx, rkey, dedupDone, s.ttl).Err(); err != nil {
		return fmt.Errorf("redis set done: %w", err)
	}
	return nil
}

// ReleaseClaim drops a claim after failed processing so the message can be
// retried immediately on redelivery instead of waiting out the claim TTL.
func (s *IdempotencyStore) ReleaseClaim(ctx context.Context, topic string, messageID string) error {
	rkey := "dedup:" + topic + ":" + messageID

	if err := s.client.Del(ctx, rkey).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}
