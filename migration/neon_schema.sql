-- Neon Schema for Incentiv Blockchain Data
-- Full chain activity capture + analytics

-- ============================================================
-- CORE TABLES
-- ============================================================

-- Blocks table
CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT PRIMARY KEY,
    hash TEXT NOT NULL,
    parent_hash TEXT,
    timestamp TIMESTAMPTZ,
    gas_used BIGINT,
    gas_limit BIGINT,
    base_fee_per_gas BIGINT,
    miner TEXT,
    difficulty TEXT,
    total_difficulty TEXT,
    size BIGINT,
    extra_data TEXT,
    nonce TEXT,
    sha3_uncles TEXT,
    logs_bloom TEXT,
    transactions_root TEXT,
    state_root TEXT,
    receipts_root TEXT,
    transaction_count INTEGER,
    chain TEXT DEFAULT 'incentiv',
    extracted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    hash TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    from_address TEXT,
    to_address TEXT,
    value TEXT,
    gas_price TEXT,
    gas BIGINT,
    gas_used BIGINT,
    input TEXT,
    input_data TEXT,
    status TEXT,
    nonce TEXT,
    transaction_index INTEGER,
    block_hash TEXT,
    block_timestamp TIMESTAMPTZ,
    timestamp TIMESTAMPTZ,
    chain TEXT DEFAULT 'incentiv',
    extracted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (block_number) REFERENCES blocks(number)
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_transactions_from ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_to ON transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);

-- ============================================================
-- RAW LOGS (ALL on-chain event logs, regardless of contract)
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_logs (
    id BIGSERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    address TEXT NOT NULL,
    topic0 TEXT,
    topic1 TEXT,
    topic2 TEXT,
    topic3 TEXT,
    data TEXT,
    block_timestamp TIMESTAMPTZ,
    chain TEXT DEFAULT 'incentiv',
    extracted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_logs_unique
    ON raw_logs(transaction_hash, log_index);
CREATE INDEX IF NOT EXISTS idx_raw_logs_block ON raw_logs(block_number);
CREATE INDEX IF NOT EXISTS idx_raw_logs_address ON raw_logs(address);
CREATE INDEX IF NOT EXISTS idx_raw_logs_topic0 ON raw_logs(topic0);
CREATE INDEX IF NOT EXISTS idx_raw_logs_timestamp ON raw_logs(block_timestamp);

-- ============================================================
-- DECODED EVENTS (events matched to known ABIs)
-- ============================================================

CREATE TABLE IF NOT EXISTS decoded_events (
    id BIGSERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    params JSONB,
    timestamp TIMESTAMPTZ,
    chain TEXT DEFAULT 'incentiv',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (block_number) REFERENCES blocks(number),
    FOREIGN KEY (transaction_hash) REFERENCES transactions(hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_decoded_events_unique
    ON decoded_events(transaction_hash, log_index);
CREATE INDEX IF NOT EXISTS idx_decoded_events_block_number ON decoded_events(block_number);
CREATE INDEX IF NOT EXISTS idx_decoded_events_contract ON decoded_events(contract_address);
CREATE INDEX IF NOT EXISTS idx_decoded_events_event_name ON decoded_events(event_name);
CREATE INDEX IF NOT EXISTS idx_decoded_events_tx_hash ON decoded_events(transaction_hash);
CREATE INDEX IF NOT EXISTS idx_decoded_events_timestamp ON decoded_events(timestamp);

-- ============================================================
-- CONTRACT DISCOVERY (tracks all contract addresses seen)
-- ============================================================

CREATE TABLE IF NOT EXISTS contracts (
    address TEXT PRIMARY KEY,
    first_seen_block BIGINT,
    last_activity_block BIGINT,
    event_count BIGINT DEFAULT 0,
    is_decoded BOOLEAN DEFAULT FALSE,
    contract_type TEXT,  -- 'erc20', 'dex', 'bridge', 'unknown'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_contracts_type ON contracts(contract_type);
CREATE INDEX IF NOT EXISTS idx_contracts_first_seen ON contracts(first_seen_block);

-- ============================================================
-- EXTRACTION STATE (replaces state.json for DB-native tracking)
-- ============================================================

CREATE TABLE IF NOT EXISTS extraction_state (
    id SERIAL PRIMARY KEY,
    extraction_type TEXT NOT NULL UNIQUE,
    last_block_processed BIGINT DEFAULT 0,
    total_items_processed BIGINT DEFAULT 0,
    status TEXT DEFAULT 'idle',  -- 'idle', 'running', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed initial extraction types
INSERT INTO extraction_state (extraction_type, last_block_processed, status)
VALUES
    ('blocks', 0, 'idle'),
    ('transactions', 0, 'idle'),
    ('raw_logs', 0, 'idle'),
    ('decoded_events', 0, 'idle'),
    ('csv_import_blocks', 0, 'idle'),
    ('csv_import_transactions', 0, 'idle'),
    ('csv_import_logs', 0, 'idle'),
    ('csv_import_decoded', 0, 'idle')
ON CONFLICT (extraction_type) DO NOTHING;

-- ============================================================
-- MATERIALIZED VIEWS (analytics aggregations)
-- ============================================================

-- Daily chain stats
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_stats AS
SELECT
    DATE_TRUNC('day', timestamp::TIMESTAMPTZ) AS day,
    COUNT(*) AS block_count,
    SUM(transaction_count) AS tx_count,
    AVG(gas_used) AS avg_gas_used,
    MAX(gas_used) AS max_gas_used,
    SUM(gas_used) AS total_gas_used
FROM blocks
WHERE timestamp IS NOT NULL
GROUP BY DATE_TRUNC('day', timestamp::TIMESTAMPTZ)
ORDER BY day;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_stats_day ON mv_daily_stats(day);

-- Daily active addresses
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_addresses AS
SELECT
    DATE_TRUNC('day', timestamp::TIMESTAMPTZ) AS day,
    COUNT(DISTINCT from_address) AS unique_senders,
    COUNT(DISTINCT to_address) AS unique_receivers,
    COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address) AS total_unique_addresses,
    COUNT(*) AS tx_count
FROM transactions
WHERE timestamp IS NOT NULL
GROUP BY DATE_TRUNC('day', timestamp::TIMESTAMPTZ)
ORDER BY day;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_addresses_day ON mv_daily_addresses(day);

-- Event type summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_summary AS
SELECT
    event_name,
    contract_address,
    COUNT(*) AS event_count,
    MIN(timestamp) AS first_seen,
    MAX(timestamp) AS last_seen,
    COUNT(DISTINCT DATE_TRUNC('day', timestamp::TIMESTAMPTZ)) AS active_days
FROM decoded_events
WHERE timestamp IS NOT NULL
GROUP BY event_name, contract_address
ORDER BY event_count DESC;

-- Contract activity summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contract_activity AS
SELECT
    address,
    COUNT(*) AS log_count,
    COUNT(DISTINCT topic0) AS unique_event_types,
    MIN(block_timestamp) AS first_activity,
    MAX(block_timestamp) AS last_activity,
    COUNT(DISTINCT DATE_TRUNC('day', block_timestamp::TIMESTAMPTZ)) AS active_days
FROM raw_logs
WHERE block_timestamp IS NOT NULL
GROUP BY address
ORDER BY log_count DESC;

-- ============================================================
-- HELPER FUNCTION: Refresh all materialized views
-- ============================================================

CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_addresses;
    REFRESH MATERIALIZED VIEW mv_event_summary;
    REFRESH MATERIALIZED VIEW mv_contract_activity;
END;
$$ LANGUAGE plpgsql;
