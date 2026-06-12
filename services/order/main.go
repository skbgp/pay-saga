package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/skbgp/saga-platform/common"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("order service starting...")

	pgURL := envOr("POSTGRES_URL", "postgres://saga:saga@localhost/saga?sslmode=disable")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	port := envOr("PORT", "8080")

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
		log.Fatalf("FATAL: topic creation failed: %v", err)
	}

	producer := common.NewProducer(kafkaBrokers, common.TopicOrderCreated)
	defer producer.Close()
	log.Println("kafka producer ready")

	idemStore := common.NewIdempotencyStore(redisAddr, 24*time.Hour)
	log.Println("redis idempotency store ready")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := newOutboxPoller(db, producer, 500*time.Millisecond)
	go poller.Start(ctx)

	handler := &orderHandler{db: db, idem: idemStore}

	mux := http.NewServeMux()
	mux.Handle("/orders", handler)
	mux.Handle("/orders/", handler)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)

		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("ERROR: server shutdown: %v", err)
		}
	}()

	log.Printf("order service listening on :%s", port)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}

	log.Println("order service stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
