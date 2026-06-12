-- PaySaga schema.

BEGIN;

CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    product_id  TEXT NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 1,
    total_cents INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox for guaranteed Kafka delivery.
-- Poller uses FOR UPDATE SKIP LOCKED to prevent duplicate publish.
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL PRIMARY KEY,
    agg_type    TEXT NOT NULL,
    agg_id      TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    status      TEXT NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, created_at);

-- Saga orchestrator state. Each row tracks one order's progress.
CREATE TABLE IF NOT EXISTS saga_state (
    order_id    TEXT PRIMARY KEY,
    step        TEXT NOT NULL DEFAULT 'CREATED',
    pay_status  TEXT NOT NULL DEFAULT 'PENDING',
    inv_status  TEXT NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Payment records for audit and duplicate detection.
CREATE TABLE IF NOT EXISTS payments (
    id          BIGSERIAL PRIMARY KEY,
    order_id    TEXT NOT NULL UNIQUE,
    amount      INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'SUCCESS',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- idx_payments_order is implicit from the UNIQUE constraint on order_id.

-- Inventory with version column for optimistic concurrency control.
CREATE TABLE IF NOT EXISTS inventory (
    product_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 0,
    version     INTEGER NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO inventory (product_id, name, quantity) VALUES
    ('WIDGET-1', 'Premium Widget', 5),
    ('WIDGET-2', 'Standard Widget', 100),
    ('WIDGET-3', 'Budget Widget', 1000)
ON CONFLICT DO NOTHING;

COMMIT;
