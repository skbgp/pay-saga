package saga

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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

// RefundPayment reverses a successful payment by marking it REFUNDED.
func (c *Compensator) RefundPayment(ctx context.Context, orderID string) error {
	log.Printf("COMPENSATE [%s]: issuing refund", orderID)

	result, err := c.db.ExecContext(ctx, `
		UPDATE payments SET status = 'REFUNDED'
		WHERE order_id = $1 AND status = 'SUCCESS'`,
		orderID,
	)
	if err != nil {
		c.logCompensationFailure(orderID, "REFUND", err)
		return fmt.Errorf("refund payment: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		log.Printf("COMPENSATE [%s]: no payment to refund (already refunded or never charged)", orderID)
		return nil
	}

	log.Printf("COMPENSATE [%s]: refund issued successfully", orderID)
	return nil
}

// UnreserveStock returns reserved inventory back to available stock.
func (c *Compensator) UnreserveStock(ctx context.Context, orderID string, productID string, quantity int) error {
	log.Printf("COMPENSATE [%s]: unreserving %d units of %s", orderID, quantity, productID)

	_, err := c.db.ExecContext(ctx, `
		UPDATE inventory
		SET quantity = quantity + $1,
		    version  = version + 1,
		    updated_at = now()
		WHERE product_id = $2`,
		quantity, productID,
	)
	if err != nil {
		c.logCompensationFailure(orderID, "UNRESERVE", err)
		return fmt.Errorf("unreserve stock: %w", err)
	}

	log.Printf("COMPENSATE [%s]: stock unreserved", orderID)
	return nil
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

func (c *Compensator) logCompensationFailure(orderID string, step string, err error) {
	log.Printf("CRITICAL: compensation failed for order %s step %s: %v",
		orderID, step, err)
}
