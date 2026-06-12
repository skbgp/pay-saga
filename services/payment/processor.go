package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/skbgp/saga-platform/common"
)

type paymentProcessor struct {
	db       *sql.DB
	consumer *kafka.Reader
	producer *kafka.Writer
	gateway  *Gateway
	dedup    *common.IdempotencyStore
}

type orderEvent struct {
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	TotalCents int    `json:"total_cents"`
}

type paymentEvent struct {
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	TotalCents int    `json:"total_cents"`
	PayStatus  string `json:"pay_status"`
	PayMsg     string `json:"pay_msg"`
}

// Start begins the consume loop. Runs until ctx is cancelled.
func (p *paymentProcessor) Start(ctx context.Context) {
	log.Println("payment processor started, consuming from:", common.TopicOrderCreated)

	for {
		msg, err := p.consumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("payment processor shutting down")
				return
			}
			log.Printf("ERROR: read message: %v", err)
			continue
		}

		p.handleMessage(ctx, msg)
	}
}

func (p *paymentProcessor) handleMessage(ctx context.Context, msg kafka.Message) {
	var order orderEvent
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		log.Printf("ERROR: unmarshal order event: %v (raw: %s)", err, string(msg.Value))
		return
	}

	// Consumer dedup: skip if already processed.
	msgID := msgIdentifier(msg)
	isDup, err := p.dedup.MarkSeen(ctx, common.TopicOrderCreated, msgID)
	if err != nil {
		log.Printf("WARN: dedup check failed: %v (processing anyway)", err)
	} else if isDup {
		log.Printf("SKIP: duplicate message for order %s (msgID=%s)", order.OrderID, msgID)
		return
	}

	log.Printf("processing payment for order %s (user=%s amount=%d)",
		order.OrderID, order.UserID, order.TotalCents)

	result := p.gateway.Charge(order.OrderID, order.TotalCents)

	log.Printf("payment result for order %s: status=%s latency=%dms msg=%s",
		order.OrderID, result.Status, result.LatencyMs, result.Message)

	payStatus := "SUCCESS"
	if !result.Success {
		payStatus = "FAILED"
	}

	_, err = p.db.ExecContext(ctx, `
		INSERT INTO payments (order_id, amount, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id) DO NOTHING`,
		order.OrderID, order.TotalCents, payStatus,
	)
	if err != nil {
		log.Printf("ERROR: insert payment record: %v", err)
	}

	topic := common.TopicPaymentCompleted
	if !result.Success {
		topic = common.TopicPaymentFailed
	}

	evt := paymentEvent{
		OrderID:    order.OrderID,
		UserID:     order.UserID,
		ProductID:  order.ProductID,
		Quantity:   order.Quantity,
		TotalCents: order.TotalCents,
		PayStatus:  payStatus,
		PayMsg:     result.Message,
	}

	evtBytes, _ := json.Marshal(evt)

	err = p.producer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(order.UserID),
		Value: evtBytes,
	})
	if err != nil {
		log.Printf("ERROR: publish payment event: %v", err)
	}
}

func msgIdentifier(msg kafka.Message) string {
	return string(msg.Topic) + ":" +
		itoa(msg.Partition) + ":" +
		ltoa(msg.Offset)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}

func ltoa(n int64) string {
	return itoa(int(n))
}
