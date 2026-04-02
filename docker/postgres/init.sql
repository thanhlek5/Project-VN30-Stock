-- VN30 Gold Layer
CREATE TABLE IF NOT EXISTS vn30_gold_layer (
    id          SERIAL PRIMARY KEY,
    time        DATE           NOT NULL,
    ticker      VARCHAR(10)    NOT NULL,
    open        NUMERIC(15, 4),
    high        NUMERIC(15, 4),
    low         NUMERIC(15, 4),
    close       NUMERIC(15, 4),
    volume      BIGINT,
    invalid     SMALLINT       DEFAULT 0,
    month       SMALLINT,
    year        SMALLINT,
    day         SMALLINT,
    -- Req 1: Price Change
    price_diff_pct_1d   NUMERIC(10, 2),
    price_diff_pct_1w   NUMERIC(10, 2),
    -- Req 2: Volume Analysis
    volume_vs_avg_20d   NUMERIC(10, 2),
    -- Req 3: Price Position
    ma20                NUMERIC(15, 4),
    above_ma20          BOOLEAN,
    dist_from_ma20      NUMERIC(10, 2),
    -- Metadata
    created_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (ticker, time)
);

CREATE INDEX IF NOT EXISTS idx_gold_ticker      ON vn30_gold_layer (ticker);
CREATE INDEX IF NOT EXISTS idx_gold_time        ON vn30_gold_layer (time);
CREATE INDEX IF NOT EXISTS idx_gold_ticker_time ON vn30_gold_layer (ticker, time);

-- Phân quyền
GRANT SELECT, INSERT, UPDATE, DELETE ON vn30_gold_layer TO admin;
GRANT USAGE, SELECT ON SEQUENCE vn30_gold_layer_id_seq TO admin;
