package saga

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/skbgp/saga-platform/common"
)

// Orchestrator coordinates saga state transitions based on Kafka events.
type Orchestrator struct {
	store       *StateStore
	compensator *Compensator
	db          *sql.DB

	paymentDone   *kafka.Reader
	paymentFailed *kafka.Reader
	inventoryDone *kafka.Reader

	dedup *common.IdempotencyStore
}

// NewOrchestrator creates an orchestrator consuming from all relevant topics.
func NewOrchestrator(db *sql.DB, brokers []string, dedup *common.IdempotencyStore) *Orchestrator {
	store := NewStateStore(db)
	comp := NewCompensator(db, store)

	return &Orchestrator{
		store:       store,
		compensator: comp,
		db:          db,
		dedup:       dedup,

		paymentDone:   common.NewConsumer(brokers, common.TopicPaymentCompleted, "saga-orchestrator"),
		paymentFailed: common.NewConsumer(brokers, common.TopicPaymentFailed, "saga-orchestrator-fail"),
		inventoryDone: common.NewConsumer(brokers, common.TopicInventoryResult, "saga-orchestrator-inv"),
	}
}

type orderCreatedEvent struct {
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	TotalCents int    `json:"total_cents"`
}

type paymentResultEvent struct {
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	TotalCents int    `json:"total_cents"`
	PayStatus  string `json:"pay_status"`
	PayMsg     string `json:"pay_msg"`
}

type inventoryResultEvent struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	ProductID string `json:"product_id"`
	Reserved  bool   `json:"reserved"`
	Message   string `json:"message"`
}

// Start launches all event consumers. Blocks until ctx is cancelled.
func (o *Orchestrator) Start(ctx context.Context) {
	log.Println("saga orchestrator starting...")

	o.resumeIncomplete(ctx)

	go o.consumePaymentCompleted(ctx)
	go o.consumePaymentFailed(ctx)
	go o.consumeInventoryResult(ctx)

	log.Println("saga orchestrator running")
	<-ctx.Done()

	o.paymentDone.Close()
	o.paymentFailed.Close()
	o.inventoryDone.Close()

	log.Println("saga orchestrator stopped")
}

// resumeIncomplete finds sagas interrupted by a previous crash.
func (o *Orchestrator) resumeIncomplete(ctx context.Context) {
	states, err := o.store.FindIncomplete(ctx)
	if err != nil {
		log.Printf("WARN: could not query incomplete sagas: %v", err)
		return
	}

	if len(states) == 0 {
		log.Println("no incomplete sagas to resume")
		return
	}

	log.Printf("found %d incomplete sagas from previous run:", len(states))
	for _, s := range states {
		log.Printf("  order=%s step=%s pay=%s inv=%s (created=%s)",
			s.OrderID, s.Step, s.PayStatus, s.InvStatus, s.CreatedAt.Format("15:04:05"))
	}
}

// consumePaymentCompleted: PAYMENT_PENDING -> INVENTORY_PENDING
func (o *Orchestrator) consumePaymentCompleted(ctx context.Context) {
	for {
		msg, err := o.paymentDone.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("ERROR: read payment.completed: %v", err)
			continue
		}

		var evt paymentResultEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("ERROR: unmarshal payment.completed: %v", err)
			continue
		}

		o.store.Create(ctx, evt.OrderID)

		log.Printf("SAGA [%s]: payment completed, advancing to INVENTORY_PENDING", evt.OrderID)

		err = o.store.Transition(ctx, evt.OrderID,
			StepInventoryPending, SubSuccess, SubPending)
		if err != nil {
			log.Printf("ERROR: transition saga %s: %v", evt.OrderID, err)
		}
	}
}

// consumePaymentFailed: PAYMENT_PENDING -> FAILED
// No inventory to undo since reservation never happened.
func (o *Orchestrator) consumePaymentFailed(ctx context.Context) {
	for {
		msg, err := o.paymentFailed.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("ERROR: read payment.failed: %v", err)
			continue
		}

		var evt paymentResultEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("ERROR: unmarshal payment.failed: %v", err)
			continue
		}

		o.store.Create(ctx, evt.OrderID)

		log.Printf("SAGA [%s]: payment failed (%s), marking order as FAILED",
			evt.OrderID, evt.PayMsg)

		// No compensation needed: payment didn't go through,
		// inventory was never reserved.
		err = o.store.Transition(ctx, evt.OrderID,
			StepFailed, SubFailed, SubPending)
		if err != nil {
			log.Printf("ERROR: transition saga %s: %v", evt.OrderID, err)
		}

		o.store.UpdateOrderStatus(ctx, evt.OrderID, "FAILED")
	}
}

// consumeInventoryResult handles reservation results.
// reserved=true  -> COMPLETED
// reserved=false -> compensate (refund payment) -> FAILED
func (o *Orchestrator) consumeInventoryResult(ctx context.Context) {
	for {
		msg, err := o.inventoryDone.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("ERROR: read inventory.result: %v", err)
			continue
		}

		var evt inventoryResultEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("ERROR: unmarshal inventory.result: %v", err)
			continue
		}

		if evt.Reserved {
			log.Printf("SAGA [%s]: inventory reserved, order COMPLETED", evt.OrderID)

			err = o.store.Transition(ctx, evt.OrderID,
				StepCompleted, SubSuccess, SubReserved)
			if err != nil {
				log.Printf("ERROR: transition saga %s: %v", evt.OrderID, err)
			}

			o.store.UpdateOrderStatus(ctx, evt.OrderID, "COMPLETED")
			log.Printf("ORDER FULFILLED: %s (product=%s)", evt.OrderID, evt.ProductID)

		} else {
			// Inventory failed after payment succeeded. Need to refund.
			log.Printf("SAGA [%s]: inventory failed (%s), triggering compensation",
				evt.OrderID, evt.Message)

			state := &State{
				OrderID:   evt.OrderID,
				PayStatus: SubSuccess,
				InvStatus: SubFailed,
			}

			err = o.compensator.RunCompensation(
				ctx, evt.OrderID, evt.ProductID, 1, state)
			if err != nil {
				log.Printf("ERROR: compensation failed for order %s: %v", evt.OrderID, err)
			}
		}
	}
}
