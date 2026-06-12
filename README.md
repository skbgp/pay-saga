# PaySaga

A distributed payment processing system built with saga orchestration, demonstrating how to maintain data consistency across microservices without distributed transactions.

## What it does

PaySaga processes payment orders across 4 independent services. If any step fails (payment declined, out of stock), the system automatically compensates by undoing previous steps (refunding payment, unreserving stock).

```
Client -> Order Service -> [Kafka] -> Payment Service -> [Kafka] -> Inventory Service
                                              |                           |
                                         Saga Orchestrator <--------------+
                                    (coordinates + compensates)
```

## Core patterns

- **Transactional Outbox**: Order + Kafka event written in the same DB transaction. Background poller publishes to Kafka in batches. No dual-write problem.
- **Saga Orchestration**: Central orchestrator tracks multi-step transactions. On failure, walks backward through compensation steps (refund payment, unreserve stock).
- **Optimistic Concurrency Control**: Inventory uses a version column to detect conflicting writes. Retries on version mismatch instead of holding locks.
- **Idempotency**: API-level (Idempotency-Key header, Stripe-style) and consumer-level (Redis SETNX dedup for Kafka messages). Payments table has a UNIQUE constraint on order_id as the final safety net.

## Quick start

```bash
# Start infrastructure
make infra

# Wait ~30s for Kafka to fully boot, then in separate terminals:
make order      # port 8080
make payment
make inventory
make saga

# Create an order
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-001" \
  -d '{"user_id":"u1","product_id":"WIDGET-1","quantity":1,"price_cents":999}'

# Check status
curl localhost:8080/orders/ord_test-001
```

## Project structure

```
pay-saga/
├── common/
│   ├── kafka.go            # Producer/consumer helpers, topic init
│   └── idempotency.go      # Redis idempotency + consumer dedup
├── saga/
│   ├── state.go            # Saga state machine + persistence
│   ├── orchestrator.go     # Event-driven saga coordinator
│   └── compensation.go     # Undo actions (refund, unreserve)
├── services/
│   ├── order/
│   │   ├── main.go         # HTTP server entrypoint
│   │   ├── handler.go      # POST /orders with idempotency
│   │   └── outbox.go       # Background outbox poller (batch)
│   ├── payment/
│   │   ├── main.go         # Kafka consumer entrypoint
│   │   ├── processor.go    # Payment event handler
│   │   └── gateway.go      # Simulated payment gateway
│   ├── inventory/
│   │   ├── main.go         # Kafka consumer entrypoint
│   │   └── processor.go    # OCC stock reservation
│   └── saga/
│       └── main.go         # Orchestrator entrypoint
├── migrations/
│   └── 001_schema.sql      # Postgres schema
├── scripts/
│   ├── verify_correctness.sh  # Functional tests
│   └── load_test.sh           # 200-request concurrent test
├── docker-compose.yml      # Kafka, Postgres, Redis
└── Makefile
```

## Design decisions

### Why transactional outbox instead of publishing directly to Kafka?

If the app crashes between inserting the order and publishing the event, the order exists but no downstream service knows about it. The outbox pattern eliminates this: both writes happen in one DB transaction. The poller retries until Kafka acknowledges.

### Why batch publishing in the outbox poller?

The poller claims up to 100 pending events using `FOR UPDATE SKIP LOCKED`, publishes them to Kafka in a single `WriteMessages` call, and marks them all as `SENT` in one bulk SQL update (`WHERE id = ANY($1)`). This reduces Kafka round-trips and Postgres I/O compared to processing one event at a time.

### Why optimistic locking instead of SELECT FOR UPDATE?

Pessimistic locks hold rows for the duration of the transaction, causing contention under load. Optimistic locking reads without locks, then does a conditional write with a version check. Conflicts are rare relative to total traffic, so the happy path is fast.

### Why a saga orchestrator instead of pure choreography?

Without a coordinator, each service must know the full transaction flow to trigger compensations. That couples services to each other. The orchestrator centralizes the "what happens next" and "what if it fails" logic, keeping services focused on their own domain.

### Outbox polling: FOR UPDATE SKIP LOCKED

If you run multiple poller instances, a naive SELECT would cause both to read the same rows. `FOR UPDATE SKIP LOCKED` claims rows atomically - other pollers skip locked rows instead of waiting. No duplicates, no deadlocks.

### Duplicate prevention at every layer

1. **API layer**: Idempotency-Key header with body hash check (prevents re-submission)
2. **Consumer layer**: Redis SETNX with topic:partition:offset key (prevents reprocessing on Kafka redelivery)
3. **Database layer**: UNIQUE constraint on `payments(order_id)` with `ON CONFLICT DO NOTHING` (final safety net if Redis is down or consumer rebalances)

## Testing

```bash
# Functional correctness
make verify

# Load test (200 concurrent orders, 50 at a time)
chmod +x scripts/load_test.sh
./scripts/load_test.sh
```

The functional test runs three scenarios:
1. **Happy path**: order -> payment -> inventory -> COMPLETED
2. **Idempotency**: same request twice returns cached response
3. **Compensation**: forced failure triggers refund + status update

The load test fires 200 concurrent orders and verifies:
- Zero duplicate payments
- All outbox events delivered (200/200 SENT)
- All sagas reach a terminal state (COMPLETED or FAILED)

## Tech stack

| Component | Technology |
|-----------|------------|
| Language  | Go 1.22    |
| Messaging | Kafka (segmentio/kafka-go) |
| Database  | PostgreSQL 16 |
| Cache     | Redis 7 |
| Infra     | Docker Compose |
