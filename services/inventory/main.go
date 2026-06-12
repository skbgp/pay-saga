package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/skbgp/saga-platform/common"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("inventory service starting...")

	pgURL := envOr("POSTGRES_URL", "postgres://saga:saga@localhost/saga?sslmode=disable")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")

	maxRetries := 3
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxRetries = n
		}
	}

	db, err := sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	log.Println("connected to postgres")

	if err := common.EnsureTopics(kafkaBrokers); err != nil {
		log.Fatalf("FATAL: kafka not ready: %v", err)
	}

	consumer := common.NewConsumer(kafkaBrokers, common.TopicPaymentCompleted, "inventory-service")
	defer consumer.Close()

	producer := common.NewProducer(kafkaBrokers, "")
	defer producer.Close()

	dedup := common.NewIdempotencyStore(redisAddr, 24*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc := &inventoryProcessor{
		db:         db,
		consumer:   consumer,
		producer:   producer,
		dedup:      dedup,
		maxRetries: maxRetries,
	}

	log.Printf("inventory service ready (max_retries=%d)", maxRetries)
	go proc.Start(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("inventory service stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
