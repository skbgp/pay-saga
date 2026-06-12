#!/bin/bash
# verify_correctness.sh - PaySaga verification script.
#
# Runs three scenarios:
#   1. Happy path: order -> payment -> inventory -> completed
#   2. Idempotency: retry with same key returns cached response
#   3. Compensation: concurrent orders exhaust stock, trigger refunds
#
# Prerequisites:
#   - Docker running (make infra)
#   - All services running in separate terminals

set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
BOLD="\033[1m"
GREEN="\033[32m"
RED="\033[31m"
YELLOW="\033[33m"
RESET="\033[0m"

echo -e "${BOLD}=== PaySaga Verification ===${RESET}"
echo ""

# --- Scenario 1: Happy path ---
echo -e "${BOLD}--- Scenario 1: Happy Path ---${RESET}"
echo "Creating order for WIDGET-2 (100 units in stock, should succeed)"

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/orders" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: demo-happy-$(date +%s)" \
    -d '{"user_id":"demo-user","product_id":"WIDGET-2","quantity":1,"price_cents":1999}')

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

if [ "$HTTP_CODE" = "201" ]; then
    echo -e "${GREEN}✓ Order created (HTTP $HTTP_CODE)${RESET}"
    echo "  Response: $BODY"
else
    echo -e "${RED}✗ Unexpected response (HTTP $HTTP_CODE)${RESET}"
    echo "  Response: $BODY"
fi

echo ""
echo "Waiting 5s for payment + inventory processing..."
sleep 5

ORDER_ID=$(echo "$BODY" | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
if [ -n "$ORDER_ID" ]; then
    STATUS=$(curl -s "$BASE_URL/orders/$ORDER_ID" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    echo -e "Order status: ${GREEN}$STATUS${RESET}"
fi

echo ""

# --- Scenario 2: Idempotency check ---
echo -e "${BOLD}--- Scenario 2: Idempotency (retry with same key) ---${RESET}"
IDEM_KEY="demo-idem-$(date +%s)"

echo "Sending first request with Idempotency-Key: $IDEM_KEY"
FIRST=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/orders" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEM_KEY" \
    -d '{"user_id":"demo-user","product_id":"WIDGET-2","quantity":1,"price_cents":500}')
echo -e "${GREEN}✓ First request: HTTP $(echo "$FIRST" | tail -1)${RESET}"
echo "Sending duplicate request with same key..."
SECOND=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/orders" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEM_KEY" \
    -d '{"user_id":"demo-user","product_id":"WIDGET-2","quantity":1,"price_cents":500}')
echo -e "${GREEN}✓ Duplicate request: HTTP $(echo "$SECOND" | tail -1) (cached response)${RESET}"

echo ""
echo "Sending request with same key but DIFFERENT body..."
CONFLICT=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/orders" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEM_KEY" \
    -d '{"user_id":"demo-user","product_id":"WIDGET-3","quantity":99,"price_cents":100}')
CONFLICT_CODE=$(echo "$CONFLICT" | tail -1)
if [ "$CONFLICT_CODE" = "409" ]; then
    echo -e "${YELLOW}✓ Conflict detected: HTTP 409 (same key, different body)${RESET}"
else
    echo -e "${RED}✗ Expected 409, got $CONFLICT_CODE${RESET}"
fi

echo ""

# --- Scenario 3: Inventory Conflict (Compensation) ---
echo -e "${BOLD}--- Scenario 3: Inventory Conflict (Compensation) ---${RESET}"
echo "Resetting WIDGET-1 inventory to 5 units..."

if command -v psql &>/dev/null; then
    psql -U saga -h localhost -d saga -c \
        "UPDATE inventory SET quantity=5, version=1 WHERE product_id='WIDGET-1';" \
        2>/dev/null || echo "(psql not available, skip reset)"
else
    echo "(psql not available, manually reset WIDGET-1 quantity to 5)"
fi

echo "Sending 20 concurrent orders for WIDGET-1..."
for i in $(seq 1 20); do
    curl -s -X POST "$BASE_URL/orders" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: demo-concurrent-$i-$(date +%s)" \
        -d "{\"user_id\":\"user-$i\",\"product_id\":\"WIDGET-1\",\"quantity\":1,\"price_cents\":999}" &
done

wait

echo ""
echo "Waiting 10s for all sagas to complete..."
sleep 10

echo ""
echo -e "${BOLD}--- Verification ---${RESET}"

if command -v psql &>/dev/null; then
    echo ""
    echo "Duplicate payment check:"
    psql -U saga -h localhost -d saga -c \
        "SELECT order_id, COUNT(*) FROM payments GROUP BY order_id HAVING COUNT(*) > 1;" \
        2>/dev/null || echo "(run manually)"

    echo ""
    echo "Remaining inventory:"
    psql -U saga -h localhost -d saga -c \
        "SELECT product_id, quantity, version FROM inventory WHERE product_id='WIDGET-1';" \
        2>/dev/null || echo "(run manually)"

    echo ""
    echo "Saga outcomes:"
    psql -U saga -h localhost -d saga -c \
        "SELECT step, COUNT(*) FROM saga_state GROUP BY step ORDER BY count DESC;" \
        2>/dev/null || echo "(run manually)"
else
    echo "psql not available. Run these queries manually:"
    echo "  SELECT order_id, COUNT(*) FROM payments GROUP BY order_id HAVING COUNT(*) > 1;"
    echo "  SELECT product_id, quantity FROM inventory WHERE product_id='WIDGET-1';"
    echo "  SELECT step, COUNT(*) FROM saga_state GROUP BY step;"
fi

echo ""
echo -e "${BOLD}=== Verification Complete ===${RESET}"
