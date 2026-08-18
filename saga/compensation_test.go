package saga

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// The compensator's retry policy is pure logic, so it can be tested without
// Kafka or a live database. The dead-letter write is pointed at an
// unreachable Postgres, which fails fast and exercises the "even the failure
// record failed" path rather than panicking on a nil handle.
func newTestCompensator(t *testing.T) *Compensator {
	t.Helper()

	db, err := sql.Open("postgres", "postgres://nobody@127.0.0.1:1/nodb?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("open stub db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return NewCompensator(db, NewStateStore(db))
}

// TestWithRetry_SucceedsAfterTransientFailures covers the case the retry
// exists for: the database blips, and the refund goes through on a later try
// instead of being abandoned.
func TestWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	c := newTestCompensator(t)

	attempts := 0
	err := c.withRetry(context.Background(), "ord_test", "REFUND", func() error {
		attempts++
		if attempts < 3 {
			return errors.New("connection reset")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

// TestWithRetry_SucceedsFirstTry makes sure the happy path doesn't pay for
// the retry machinery.
func TestWithRetry_SucceedsFirstTry(t *testing.T) {
	c := newTestCompensator(t)

	attempts := 0
	start := time.Now()

	err := c.withRetry(context.Background(), "ord_test", "UNRESERVE", func() error {
		attempts++
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", attempts)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("a successful compensation should not sleep, took %s", elapsed)
	}
}

// TestWithRetry_GivesUpAndReports checks the bad ending: after exhausting
// retries the compensator must surface an error rather than reporting success,
// because at that point the customer's money really is in the wrong place.
func TestWithRetry_GivesUpAndReports(t *testing.T) {
	c := newTestCompensator(t)

	attempts := 0
	permanent := errors.New("constraint violation")

	err := c.withRetry(context.Background(), "ord_test", "REFUND", func() error {
		attempts++
		return permanent
	})

	if err == nil {
		t.Fatal("expected an error after exhausting retries, got nil: a caller " +
			"would believe the refund succeeded")
	}
	if attempts != compensationMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", compensationMaxAttempts, attempts)
	}
	if !errors.Is(err, permanent) {
		t.Fatalf("underlying cause was lost, got %v", err)
	}
}

// TestWithRetry_StopsOnContextCancellation makes sure shutdown isn't delayed
// by a compensation sitting in a backoff sleep.
func TestWithRetry_StopsOnContextCancellation(t *testing.T) {
	c := newTestCompensator(t)

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	err := c.withRetry(ctx, "ord_test", "REFUND", func() error {
		attempts++
		if attempts == 1 {
			cancel() // cancel during the first backoff
		}
		return errors.New("still failing")
	})

	if err == nil {
		t.Fatal("expected an error when the context is cancelled")
	}
	if attempts >= compensationMaxAttempts {
		t.Fatalf("cancellation was ignored: ran %d attempts", attempts)
	}
}
