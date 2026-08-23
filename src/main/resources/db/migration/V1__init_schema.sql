-- =========================================================
-- 1. ORDERS - estado mutable, se actualiza incrementalmente
-- =========================================================
CREATE TABLE orders (
    numeric_order_id            BIGINT PRIMARY KEY,
    ticker                       VARCHAR(20) NOT NULL,
    side                         VARCHAR(10) NOT NULL,
    status                       VARCHAR(20) NOT NULL,
    nominal_amounts              NUMERIC(18,4) NOT NULL,
    leaves_nominal_amount        NUMERIC(18,4) NOT NULL,
    accumulative_nominal_amount  NUMERIC(18,4) NOT NULL DEFAULT 0,
    executions_count             INT NOT NULL DEFAULT 0,
    created_at                   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMP NOT NULL DEFAULT now()
);

-- =========================================================
-- 2. EXECUTION_LEDGER - append-only, una fila por ER aplicado
-- =========================================================
CREATE TABLE execution_ledger (
    id                        BIGSERIAL PRIMARY KEY,
    fix_id                    BIGINT NOT NULL,
    numeric_order_id          BIGINT NOT NULL REFERENCES orders(numeric_order_id),
    status_applied            VARCHAR(20) NOT NULL,
    execution_price           NUMERIC(18,4),
    execution_nominal_amount  NUMERIC(18,4),
    secondary_trade_id        VARCHAR(64) NOT NULL,
    operation_number          VARCHAR(64) NOT NULL,
    transaction_time          TIMESTAMP,
    applied_at                TIMESTAMP NOT NULL DEFAULT now(),

    -- Clave de dedup: el PDF marca explicitamente secondaryTradeId y
    -- operationNumber como "identidad de la ejecucion (para dedup)".
    -- fix_id se conserva como dato informativo pero NO es la clave de unicidad.
    CONSTRAINT uq_ledger_dedup_key UNIQUE (secondary_trade_id, operation_number)
);

CREATE INDEX idx_ledger_order_id ON execution_ledger(numeric_order_id);

-- =========================================================
-- 3. OUTBOX - settlement, misma tx que el UPDATE de la orden
-- =========================================================
CREATE TABLE outbox (
    id                  BIGSERIAL PRIMARY KEY,
    numeric_order_id    BIGINT NOT NULL,
    event_type          VARCHAR(30) NOT NULL DEFAULT 'SETTLEMENT',
    payload             JSONB NOT NULL,
    published           BOOLEAN NOT NULL DEFAULT false,
    created_at          TIMESTAMP NOT NULL DEFAULT now(),
    published_at        TIMESTAMP,

    CONSTRAINT uq_outbox_order_settlement UNIQUE (numeric_order_id, event_type)
);

CREATE INDEX idx_outbox_pending ON outbox(published) WHERE published = false;
