# PaySaga - build and run commands.
#
# Usage:
#   make infra      - start Kafka, Postgres, Redis
#   make infra-down - stop infrastructure
#   make order      - run the order service
#   make payment    - run the payment service
#   make inventory  - run the inventory service
#   make saga       - run the saga orchestrator
#   make verify     - run correctness checks
#   make deps       - download Go dependencies

.PHONY: deps infra infra-down order payment inventory saga verify clean

deps:
	go mod tidy

# Start infrastructure (Kafka, Postgres, Redis).
infra:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@echo "Infrastructure ready."

infra-down:
	docker-compose down -v

order:
	go run ./services/order/

payment:
	go run ./services/payment/

inventory:
	go run ./services/inventory/

saga:
	go run ./services/saga/

verify:
	chmod +x scripts/verify_correctness.sh
	./scripts/verify_correctness.sh

clean:
	rm -rf bin/
