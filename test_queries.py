from src.loaders.neon import NeonLoader
neon = NeonLoader()

print("Testing daily txs...")
try:
    rows = neon.query("""
        SELECT
            DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day,
            COUNT(*) as txs,
            AVG(gas_used * CAST(NULLIF(gas_price, '') AS numeric)) / 1e18 as avg_tx_fee_cent,
            SUM(gas_used * CAST(NULLIF(gas_price, '') AS numeric)) / 1e18 as total_tx_fee_cent
        FROM transactions
        WHERE timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1
        ORDER BY 1
        LIMIT 5
    """)
    print("txs:", rows)
except Exception as e:
    print("tx error:", e)

print("Testing daily blocks...")
try:
    rows = neon.query("""
        SELECT
            DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day,
            COUNT(*) as blocks,
            AVG(size) as avg_size,
            AVG(gas_limit) as avg_gas_limit,
            SUM(gas_used) as sum_gas_used
        FROM blocks
        WHERE timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1
        ORDER BY 1
        LIMIT 5
    """)
    print("blocks:", rows)
except Exception as e:
    print("blocks error:", e)

neon.close()
