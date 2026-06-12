package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

// outboxPoller reads pending events from Postgres and publishes to Kafka.
type outboxPoller struct {
	db       *sql.DB
	producer *kafka.Writer
	interval time.Duration
}

type outboxEvent struct {
	ID        int64
	AggType   string
	AggID     string
	EventType string
	Payload   json.RawMessage
}

func newOutboxPoller(db *sql.DB, producer *kafka.Writer, interval time.Duration) *outboxPoller {
	return &outboxPoller{
		db:       db,
		producer: producer,
		interval: interval,
	}
}

// Start begins the polling loop. Runs until ctx is cancelled.
func (p *outboxPoller) Start(ctx context.Context) {
	log.Printf("outbox poller started (interval=%s)", p.interval)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("outbox poller shutting down")
			return
		case <-ticker.C:
			if err := p.poll(ctx); err != nil {
				log.Printf("ERROR: outbox poll: %v", err)
			}
		}
	}
}

// poll claims pending events with FOR UPDATE SKIP LOCKED,
// publishes each to Kafka, and marks them SENT or FAILED.
func (p *outboxPoller) poll(ctx context.Context) error {
	rows, err := p.db.QueryContext(ctx, `
		UPDATE outbox SET status = 'PROCESSING'
		WHERE id IN (
			SELECT id FROM outbox
			WHERE status = 'PENDING'
			   OR (status = 'FAILED' AND retry_count < 10)
			ORDER BY created_at
			LIMIT 100
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, agg_type, agg_id, event_type, payload
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var events []outboxEvent
	for rows.Next() {
		var e outboxEvent
		if err := rows.Scan(&e.ID, &e.AggType, &e.AggID, &e.EventType, &e.Payload); err != nil {
			return err
		}
		events = append(events, e)
	}

	if len(events) == 0 {
		return nil
	}

	log.Printf("outbox: claimed %d events", len(events))

	var messages []kafka.Message
	var ids []int64
	for _, e := range events {
		messages = append(messages, kafka.Message{
			Key:   []byte(e.AggID),
			Value: e.Payload,
		})
		ids = append(ids, e.ID)
	}

	err = p.producer.WriteMessages(ctx, messages...)
	if err != nil {
		log.Printf("ERROR: bulk publish failed: %v", err)
		for _, id := range ids {
			p.markFailed(ctx, id)
		}
		return err
	}

	p.markSentBulk(ctx, ids)

	return nil
}

func (p *outboxPoller) markSentBulk(ctx context.Context, ids []int64) {
	if len(ids) == 0 {
		return
	}
	_, err := p.db.ExecContext(ctx,
		"UPDATE outbox SET status = 'SENT' WHERE id = ANY($1)", pq.Array(ids))
	if err != nil {
		log.Printf("WARN: mark bulk sent failed: %v", err)
	}
}

func (p *outboxPoller) markFailed(ctx context.Context, id int64) {
	_, err := p.db.ExecContext(ctx, `
		UPDATE outbox SET status = 'FAILED', retry_count = retry_count + 1
		WHERE id = $1`, id)
	if err != nil {
		log.Printf("WARN: mark failed for event %d: %v", id, err)
	}
}
