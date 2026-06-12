package common

import (
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Topic names used across services.
const (
	TopicOrderCreated     = "order.created"
	TopicPaymentCompleted = "payment.completed"
	TopicPaymentFailed    = "payment.failed"
	TopicInventoryResult  = "inventory.result"
)

// NewProducer creates a Kafka writer for a specific topic.
// Uses hash-based partitioning so all messages with the same key
// land on the same partition (per-user ordering).
func NewProducer(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
}

// NewConsumer creates a Kafka reader for a consumer group.
func NewConsumer(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        3 * time.Second,
	})
}

// EnsureTopics creates the required topics if they don't exist.
// Retries on connection failure since Kafka might still be booting.
func EnsureTopics(brokers []string) error {
	var conn *kafka.Conn
	var err error

	// Wait up to 120 seconds for Kafka to be fully ready
	for i := 0; i < 60; i++ {
		conn, err = kafka.Dial("tcp", brokers[0])
		if err == nil {
			// Verify we can fetch brokers (meaning Kafka is fully booted)
			_, err = conn.Brokers()
			if err == nil {
				break
			}
			conn.Close()
		}
		log.Printf("waiting for kafka to be ready (attempt %d): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return fmt.Errorf("kafka not ready: %w", err)
	}
	defer conn.Close()

	topics := []kafka.TopicConfig{
		{Topic: TopicOrderCreated, NumPartitions: 6, ReplicationFactor: 1},
		{Topic: TopicPaymentCompleted, NumPartitions: 6, ReplicationFactor: 1},
		{Topic: TopicPaymentFailed, NumPartitions: 6, ReplicationFactor: 1},
		{Topic: TopicInventoryResult, NumPartitions: 6, ReplicationFactor: 1},
	}

	for i := 0; i < 5; i++ {
		err = conn.CreateTopics(topics...)
		if err == nil {
			break
		}
		log.Printf("create topics attempt %d: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return err
	}

	// Wait until topics are visible in broker metadata.
	// CreateTopics can return before the controller has finished propagating.
	log.Println("topics created, waiting for metadata visibility...")
	for i := 0; i < 30; i++ {
		partitions, pErr := conn.ReadPartitions(TopicOrderCreated)
		if pErr == nil && len(partitions) > 0 {
			log.Printf("topics visible (%d partitions confirmed)", len(partitions))
			return nil
		}
		time.Sleep(time.Second)
	}

	log.Println("WARN: topic metadata check timed out, proceeding anyway")
	return nil
}
