package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/skbgp/saga-platform/common"
	"github.com/skbgp/saga-platform/saga"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("saga orchestrator service starting...")

	pgURL := envOr("POSTGRES_URL", "postgres://saga:saga@localhost/saga?sslmode=disable")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")

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

	dedup := common.NewIdempotencyStore(redisAddr, 24*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	orch := saga.NewOrchestrator(db, kafkaBrokers, dedup)
	go orch.Start(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("saga orchestrator service stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
