#!/bin/bash

BASE_URL="http://localhost:8080"
CONCURRENCY=50
TOTAL_REQUESTS=200

echo "Starting heavy load test for PaySaga..."
echo "Concurrency: $CONCURRENCY, Total Requests: $TOTAL_REQUESTS"

# Reset inventory to a large number
docker exec saga-platform-github-postgres-1 psql -U saga -h localhost -d saga -c "UPDATE inventory SET quantity=1000000, version=1 WHERE product_id='WIDGET-2';"

# Run requests concurrently using bash background jobs instead of xargs
for i in $(seq 1 $TOTAL_REQUESTS); do
    curl -s -X POST "$BASE_URL/orders" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $(uuidgen)-$i-$(date +%s%N)" \
        -d "{\"user_id\":\"user-load-$i\",\"product_id\":\"WIDGET-2\",\"quantity\":1,\"price_cents\":999}" > /dev/null &
    
    # Throttle concurrency
    if (( i % CONCURRENCY == 0 )); then
        wait
    fi
done

wait

echo "All $TOTAL_REQUESTS requests sent. Waiting for processing..."
sleep 60

# Verify correctness
echo ""
echo "--- DB VERIFICATION ---"

echo "1. Any duplicate payments?"
docker exec saga-platform-github-postgres-1 psql -U saga -h localhost -d saga -c "SELECT order_id, COUNT(*) FROM payments GROUP BY order_id HAVING COUNT(*) > 1;"

echo "2. Total orders vs Total payments vs Total sagas"
docker exec saga-platform-github-postgres-1 psql -U saga -h localhost -d saga -c "SELECT (SELECT COUNT(*) FROM orders) as orders, (SELECT COUNT(*) FROM payments) as payments, (SELECT COUNT(*) FROM saga_state) as sagas;"

echo "3. Final saga states"
docker exec saga-platform-github-postgres-1 psql -U saga -h localhost -d saga -c "SELECT step, COUNT(*) FROM saga_state GROUP BY step;"

echo "4. Outbox events processed correctly?"
docker exec saga-platform-github-postgres-1 psql -U saga -h localhost -d saga -c "SELECT status, COUNT(*) FROM outbox GROUP BY status;"

echo "Load test complete."
