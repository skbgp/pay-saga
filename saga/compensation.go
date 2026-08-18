package saga

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Compensation retry policy. A failed undo is money that never made it back
// to the customer, so it is retried with backoff before being parked for
// manual intervention.
const (
	compensationMaxAttempts = 5
	compensationBaseDelay   = 100 * time.Millisecond
	compensationMaxDelay    = 2 * time.Second
)

// Compensator executes undo actions when saga steps fail.
type Compensator struct {
	db    *sql.DB
	store *StateStore
}

// NewCompensator creates a compensator backed by the given database.
func NewCompensator(db *sql.DB, store *StateStore) *Compensator {
	return &Compensator{db: db, store: store}
}

// withRetry runs a compensation step until it succeeds or the attempts run
// out, backing off exponentially between tries.
//
// Compensation actions are idempotent by construction — the refund only
// touches rows still in SUCCESS, and unreserve is guarded by the saga state —
// so retrying one is safe.
func (c *Compensator) withRetry(ctx context.Context, orderID string, step string, action func() error) error {
	delay := compensationBaseDelay

	var lastErr error
	for attempt := 1; attempt <= compensationMaxAttempts; attempt++ {
		lastErr = action()
		if lastErr == nil {
			if attempt > 1 {
				log.Printf("COMPENSATE [%s]: %s succeeded on attempt %d", orderID, step, attempt)
			}
			return nil
		}

		log.Printf("COMPENSATE [%s]: %s failed on attempt %d/%d: %v",
			orderID, step, attempt, compensationMaxAttempts, lastErr)

		if attempt == compensationMaxAttempts {
			break
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("%s cancelled after %d attempts: %w", step, attempt, lastErr)
		}

		delay *= 2
		if delay > compensationMaxDelay {
			delay = compensationMaxDelay
		}
	}

	// Out of retries. Park it durably so it can be found and settled by hand
	// rather than disappearing into the log stream.
	c.recordCompensationFailure(ctx, orderID, step, lastErr)
	return fmt.Errorf("%s failed after %d attempts: %w", step, compensationMaxAttempts, lastErr)
}

// recordCompensationFailure writes an unrecoverable compensation into a table
// an operator can query. This is the dead-letter path: the money is in the
// wrong place and no amount of retrying has fixed it.
func (c *Compensator) recordCompensationFailure(ctx context.Context, orderID string, step string, cause error) {
	log.Printf("CRITICAL: compensation permanently failed for order %s step %s: %v",
		orderID, step, cause)

	_, err := c.db.ExecContext(ctx, `
		INSERT INTO compensation_failures (order_id, step, error, attempts)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (order_id, step) DO UPDATE
		SET error      = EXCLUDED.error,
		    attempts   = compensation_failures.attempts + EXCLUDED.attempts,
		    updated_at = now()`,
		orderID, step, cause.Error(), compensationMaxAttempts,
	)
	if err != nil {
		// Nothing left to do but shout: both the action and the record of its
		// failure are broken.
		log.Printf("CRITICAL: could not record compensation failure for order %s: %v",
			orderID, err)
	}
}

// RefundPayment reverses a successful payment by marking it REFUNDED.
// Retried on failure, and dead-lettered if it never succeeds.
func (c *Compensator) RefundPayment(ctx context.Context, orderID string) error {
	log.Printf("COMPENSATE [%s]: issuing refund", orderID)

	return c.withRetry(ctx, orderID, "REFUND", func() error {
		result, err := c.db.ExecContext(ctx, `
			UPDATE payments SET status = 'REFUNDED'
			WHERE order_id = $1 AND status = 'SUCCESS'`,
			orderID,
		)
		if err != nil {
			return fmt.Errorf("refund payment: %w", err)
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("COMPENSATE [%s]: no payment to refund (already refunded or never charged)", orderID)
			return nil
		}

		log.Printf("COMPENSATE [%s]: refund issued successfully", orderID)
		return nil
	})
}

// UnreserveStock returns reserved inventory back to available stock.
// Retried on failure, and dead-lettered if it never succeeds.
func (c *Compensator) UnreserveStock(ctx context.Context, orderID string, productID string, quantity int) error {
	log.Printf("COMPENSATE [%s]: unreserving %d units of %s", orderID, quantity, productID)

	return c.withRetry(ctx, orderID, "UNRESERVE", func() error {
		_, err := c.db.ExecContext(ctx, `
			UPDATE inventory
			SET quantity = quantity + $1,
			    version  = version + 1,
			    updated_at = now()
			WHERE product_id = $2`,
			quantity, productID,
		)
		if err != nil {
			return fmt.Errorf("unreserve stock: %w", err)
		}

		log.Printf("COMPENSATE [%s]: stock unreserved", orderID)
		return nil
	})
}

// RunCompensation walks backward through completed steps and undoes them.
// Order: unreserve stock first, then refund payment.
func (c *Compensator) RunCompensation(ctx context.Context, orderID string, productID string, quantity int, state *State) error {
	log.Printf("COMPENSATE [%s]: starting compensation (pay=%s inv=%s)",
		orderID, state.PayStatus, state.InvStatus)

	err := c.store.Transition(ctx, orderID, StepCompensating, state.PayStatus, state.InvStatus)
	if err != nil {
		return err
	}

	// Step 1: Unreserve inventory if it was reserved.
	invStatus := state.InvStatus
	if state.InvStatus == SubReserved {
		if err := c.UnreserveStock(ctx, orderID, productID, quantity); err != nil {
			log.Printf("ERROR: compensation failed for order %s: %v", orderID, err)
			// Don't return - try to refund payment anyway.
		} else {
			invStatus = SubUnreserved
		}
	}

	// Step 2: Refund payment if it succeeded.
	payStatus := state.PayStatus
	if state.PayStatus == SubSuccess {
		if err := c.RefundPayment(ctx, orderID); err != nil {
			log.Printf("ERROR: refund failed for order %s: %v", orderID, err)
		} else {
			payStatus = SubRefunded
		}
	}

	// Step 3: Mark saga as FAILED (terminal state).
	err = c.store.Transition(ctx, orderID, StepFailed, payStatus, invStatus)
	if err != nil {
		return err
	}

	// Step 4: Update order status for the API.
	c.store.UpdateOrderStatus(ctx, orderID, "FAILED")

	log.Printf("COMPENSATE [%s]: compensation complete", orderID)
	return nil
}

// HasUnresolvedFailures reports whether any compensation is still parked
// awaiting manual resolution. Surfaced on the status endpoint so a stuck
// refund is visible rather than buried in logs.
func (c *Compensator) HasUnresolvedFailures(ctx context.Context) (int, error) {
	var count int
	err := c.db.QueryRowContext(ctx,
		`SELECT count(*) FROM compensation_failures WHERE resolved = false`,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count compensation failures: %w", err)
	}
	return count, nil
}
