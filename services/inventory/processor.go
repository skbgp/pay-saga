package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/skbgp/saga-platform/common"
)

type inventoryProcessor struct {
	db       *sql.DB
	consumer *kafka.Reader
	producer *kafka.Writer
	dedup    *common.IdempotencyStore

	maxRetries int
}

type paymentCompletedEvent struct {
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	TotalCents int    `json:"total_cents"`
}

type inventoryResultEvent struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	ProductID string `json:"product_id"`
	Reserved  bool   `json:"reserved"`
	Message   string `json:"message"`
}

// Start begins the consume loop.
func (p *inventoryProcessor) Start(ctx context.Context) {
	log.Println("inventory processor started, consuming from:", common.TopicPaymentCompleted)

	for {
		msg, err := p.consumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("inventory processor shutting down")
				return
			}
			log.Printf("ERROR: read message: %v", err)
			continue
		}

		p.handleMessage(ctx, msg)
	}
}

func (p *inventoryProcessor) handleMessage(ctx context.Context, msg kafka.Message) {
	var evt paymentCompletedEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		log.Printf("ERROR: unmarshal payment event: %v", err)
		return
	}

	msgID := fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
	isDup, err := p.dedup.MarkSeen(ctx, common.TopicPaymentCompleted, msgID)
	if err != nil {
		log.Printf("WARN: dedup check failed: %v", err)
	} else if isDup {
		log.Printf("SKIP: duplicate message for order %s", evt.OrderID)
		return
	}

	log.Printf("reserving inventory for order %s (product=%s qty=%d)",
		evt.OrderID, evt.ProductID, evt.Quantity)

	reserved, reserveMsg := p.reserveStock(ctx, evt.ProductID, evt.Quantity)

	log.Printf("inventory result for order %s: reserved=%t msg=%s",
		evt.OrderID, reserved, reserveMsg)

	result := inventoryResultEvent{
		OrderID:   evt.OrderID,
		UserID:    evt.UserID,
		ProductID: evt.ProductID,
		Reserved:  reserved,
		Message:   reserveMsg,
	}

	resultBytes, _ := json.Marshal(result)

	err = p.producer.WriteMessages(ctx, kafka.Message{
		Topic: common.TopicInventoryResult,
		Key:   []byte(evt.UserID),
		Value: resultBytes,
	})
	if err != nil {
		log.Printf("ERROR: publish inventory result: %v", err)
	}
}

// reserveStock decrements stock using optimistic concurrency control.
// Retries on version conflict up to maxRetries times.
func (p *inventoryProcessor) reserveStock(ctx context.Context, productID string, qty int) (bool, string) {
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		var currentQty, currentVersion int
		err := p.db.QueryRowContext(ctx,
			"SELECT quantity, version FROM inventory WHERE product_id = $1",
			productID,
		).Scan(&currentQty, &currentVersion)

		if err == sql.ErrNoRows {
			return false, fmt.Sprintf("product %s not found", productID)
		}
		if err != nil {
			return false, fmt.Sprintf("db error: %v", err)
		}

		if currentQty < qty {
			return false, fmt.Sprintf("insufficient stock: have %d, need %d", currentQty, qty)
		}

		// Conditional update with version check (optimistic lock).
		result, err := p.db.ExecContext(ctx, `
			UPDATE inventory
			SET quantity   = quantity - $1,
			    version    = version + 1,
			    updated_at = now()
			WHERE product_id = $2
			  AND version = $3
			  AND quantity >= $1`,
			qty, productID, currentVersion,
		)
		if err != nil {
			return false, fmt.Sprintf("update error: %v", err)
		}

		rowsAffected, _ := result.RowsAffected()

		if rowsAffected == 1 {
			return true, fmt.Sprintf("reserved %d units of %s (version %d -> %d)",
				qty, productID, currentVersion, currentVersion+1)
		}

		log.Printf("RETRY: version conflict for product %s (attempt %d/%d, version was %d)",
			productID, attempt+1, p.maxRetries, currentVersion)
	}

	return false, fmt.Sprintf("version conflict after %d retries", p.maxRetries)
}
