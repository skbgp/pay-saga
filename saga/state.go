package saga

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Step represents the current position in the saga state machine.
//
//	CREATED -> PAYMENT_PENDING -> INVENTORY_PENDING -> COMPLETED
//	               |                    |
//	          COMPENSATING <--------COMPENSATING
//	               |
//	             FAILED
type Step string

const (
	StepCreated          Step = "CREATED"
	StepPaymentPending   Step = "PAYMENT_PENDING"
	StepInventoryPending Step = "INVENTORY_PENDING"
	StepCompleted        Step = "COMPLETED"
	StepCompensating     Step = "COMPENSATING"
	StepFailed           Step = "FAILED"
)

// SubStatus tracks individual step outcomes within the saga.
type SubStatus string

const (
	SubPending    SubStatus = "PENDING"
	SubSuccess    SubStatus = "SUCCESS"
	SubFailed     SubStatus = "FAILED"
	SubRefunded   SubStatus = "REFUNDED"
	SubReserved   SubStatus = "RESERVED"
	SubUnreserved SubStatus = "UNRESERVED"
)

// State holds the full state of a single saga instance.
type State struct {
	OrderID   string
	Step      Step
	PayStatus SubStatus
	InvStatus SubStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// IsTerminal returns true if the saga has reached a final state.
func (s *State) IsTerminal() bool {
	return s.Step == StepCompleted || s.Step == StepFailed
}

// StateStore persists saga state in Postgres.
type StateStore struct {
	db *sql.DB
}

// NewStateStore creates a store backed by the given Postgres connection.
func NewStateStore(db *sql.DB) *StateStore {
	return &StateStore{db: db}
}

// Create inserts a new saga for an order.
func (s *StateStore) Create(ctx context.Context, orderID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO saga_state (order_id, step, pay_status, inv_status)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (order_id) DO NOTHING`,
		orderID, StepCreated, SubPending, SubPending,
	)
	if err != nil {
		return fmt.Errorf("create saga: %w", err)
	}
	return nil
}

// Get retrieves the current saga state for an order.
func (s *StateStore) Get(ctx context.Context, orderID string) (*State, error) {
	var state State
	err := s.db.QueryRowContext(ctx, `
		SELECT order_id, step, pay_status, inv_status, created_at, updated_at
		FROM saga_state
		WHERE order_id = $1`,
		orderID,
	).Scan(
		&state.OrderID, &state.Step, &state.PayStatus,
		&state.InvStatus, &state.CreatedAt, &state.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get saga: %w", err)
	}
	return &state, nil
}

// Transition updates the saga to a new step and sub-statuses.
func (s *StateStore) Transition(ctx context.Context, orderID string, step Step, payStatus, invStatus SubStatus) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE saga_state
		SET step = $2, pay_status = $3, inv_status = $4, updated_at = now()
		WHERE order_id = $1`,
		orderID, step, payStatus, invStatus,
	)
	if err != nil {
		return fmt.Errorf("transition saga: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("saga not found for order %s", orderID)
	}

	log.Printf("SAGA [%s]: %s (pay=%s inv=%s)", orderID, step, payStatus, invStatus)
	return nil
}

// UpdateOrderStatus keeps the orders table in sync with the saga outcome.
func (s *StateStore) UpdateOrderStatus(ctx context.Context, orderID string, status string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE orders SET status = $2, updated_at = now()
		WHERE id = $1`,
		orderID, status,
	)
	return err
}

// FindIncomplete returns all sagas not in a terminal state.
// Used on startup to resume interrupted sagas after a crash.
func (s *StateStore) FindIncomplete(ctx context.Context) ([]State, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT order_id, step, pay_status, inv_status, created_at, updated_at
		FROM saga_state
		WHERE step NOT IN ($1, $2)
		ORDER BY created_at`,
		StepCompleted, StepFailed,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var states []State
	for rows.Next() {
		var st State
		if err := rows.Scan(&st.OrderID, &st.Step, &st.PayStatus, &st.InvStatus, &st.CreatedAt, &st.UpdatedAt); err != nil {
			return nil, err
		}
		states = append(states, st)
	}
	return states, nil
}
