#!/usr/bin/env python3
"""
Extract unique addresses from transaction data and decoded events.

Sources:
  1. transactions.from_address / to_address
  2. decoded_events params (sender, recipient, from, to, paymaster, etc.)

Creates/populates an `addresses` table with unique addresses and metadata.
"""

import os
import sys
import time
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: Set NEON_DATABASE_URL or DATABASE_URL environment variable")
    sys.exit(1)

BATCH_SIZE = 10000

def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.cursor().execute("SET statement_timeout = '300000'")
    conn.commit()
    return conn

def setup_table(conn):
    """Recreate addresses table with proper schema."""
    with conn.cursor() as cur:
        # Drop old table (had incompatible schema from BlockScout backfill)
        cur.execute("DROP TABLE IF EXISTS addresses CASCADE;")
        cur.execute("""
            CREATE TABLE addresses (
                address TEXT PRIMARY KEY,
                first_seen TIMESTAMPTZ,
                last_seen TIMESTAMPTZ,
                tx_count INT DEFAULT 0,
                is_contract BOOLEAN DEFAULT FALSE,
                source TEXT DEFAULT 'extracted'
            );
            CREATE INDEX idx_addresses_tx_count ON addresses(tx_count DESC);
        """)
    conn.commit()
    print("Table 'addresses' created (fresh).")

def extract_from_transactions(conn):
    """Extract unique from/to addresses from transactions table."""
    print("\n--- Extracting addresses from transactions ---")
    with conn.cursor() as cur:
        # Count first
        cur.execute("SELECT COUNT(*) FROM transactions")
        total = cur.fetchone()[0]
        print(f"Total transactions: {total:,}")

        # Extract from_address
        print("Extracting from_address...")
        cur.execute("""
            INSERT INTO addresses (address, first_seen, last_seen, tx_count)
            SELECT
                LOWER(from_address),
                MIN(block_timestamp::timestamptz),
                MAX(block_timestamp::timestamptz),
                COUNT(*)::int
            FROM transactions
            WHERE from_address IS NOT NULL
            GROUP BY LOWER(from_address)
            ON CONFLICT (address) DO UPDATE SET
                first_seen = LEAST(addresses.first_seen, EXCLUDED.first_seen),
                last_seen = GREATEST(addresses.last_seen, EXCLUDED.last_seen),
                tx_count = addresses.tx_count + EXCLUDED.tx_count
        """)
        from_count = cur.rowcount
        conn.commit()
        print(f"  Upserted {from_count:,} from_address records")

        # Extract to_address
        print("Extracting to_address...")
        cur.execute("""
            INSERT INTO addresses (address, first_seen, last_seen, tx_count)
            SELECT
                LOWER(to_address),
                MIN(block_timestamp::timestamptz),
                MAX(block_timestamp::timestamptz),
                COUNT(*)::int
            FROM transactions
            WHERE to_address IS NOT NULL
            GROUP BY LOWER(to_address)
            ON CONFLICT (address) DO UPDATE SET
                first_seen = LEAST(addresses.first_seen, EXCLUDED.first_seen),
                last_seen = GREATEST(addresses.last_seen, EXCLUDED.last_seen),
                tx_count = addresses.tx_count + EXCLUDED.tx_count
        """)
        to_count = cur.rowcount
        conn.commit()
        print(f"  Upserted {to_count:,} to_address records")

def extract_from_events(conn):
    """Extract unique addresses from decoded_events params (JSON fields)."""
    print("\n--- Extracting addresses from decoded events ---")

    # Common param keys that contain addresses
    address_fields = [
        ('sender', "params->>'sender'"),
        ('recipient', "params->>'recipient'"),
        ('from', "params->>'from'"),
        ('to', "params->>'to'"),
        ('paymaster', "params->>'paymaster'"),
        ('owner', "params->>'owner'"),
    ]

    with conn.cursor() as cur:
        for field_name, field_expr in address_fields:
            print(f"Extracting '{field_name}' field...")
            try:
                cur.execute(f"""
                    INSERT INTO addresses (address, first_seen, last_seen, tx_count)
                    SELECT
                        LOWER({field_expr}),
                        MIN("timestamp"),
                        MAX("timestamp"),
                        COUNT(*)::int
                    FROM decoded_events
                    WHERE {field_expr} IS NOT NULL
                      AND LENGTH({field_expr}) = 42
                      AND {field_expr} LIKE '0x%'
                    GROUP BY LOWER({field_expr})
                    ON CONFLICT (address) DO UPDATE SET
                        first_seen = LEAST(addresses.first_seen, EXCLUDED.first_seen),
                        last_seen = GREATEST(addresses.last_seen, EXCLUDED.last_seen),
                        tx_count = addresses.tx_count + EXCLUDED.tx_count
                """)
                count = cur.rowcount
                conn.commit()
                print(f"  Upserted {count:,} addresses from '{field_name}'")
            except Exception as e:
                conn.rollback()
                print(f"  Warning: Failed to extract '{field_name}': {e}")

def mark_contracts(conn):
    """Mark known contracts in the addresses table."""
    print("\n--- Marking known contracts ---")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE addresses
            SET is_contract = TRUE
            WHERE address IN (SELECT LOWER(address) FROM contracts)
        """)
        count = cur.rowcount
        conn.commit()
        print(f"  Marked {count:,} addresses as contracts")

def print_summary(conn):
    """Print extraction summary."""
    print("\n=== EXTRACTION SUMMARY ===")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM addresses")
        total = cur.fetchone()[0]
        print(f"Total unique addresses: {total:,}")

        cur.execute("SELECT COUNT(*) FROM addresses WHERE is_contract = TRUE")
        contracts = cur.fetchone()[0]
        print(f"Contracts: {contracts:,}")
        print(f"EOA (non-contract): {total - contracts:,}")

        cur.execute("SELECT MIN(first_seen), MAX(last_seen) FROM addresses")
        first, last = cur.fetchone()
        print(f"Date range: {first} to {last}")

        cur.execute("""
            SELECT address, tx_count
            FROM addresses
            WHERE is_contract = FALSE
            ORDER BY tx_count DESC
            LIMIT 10
        """)
        print("\nTop 10 most active EOA addresses:")
        for addr, count in cur.fetchall():
            print(f"  {addr[:10]}...{addr[-6:]}  tx_count={count:,}")

def main():
    print("Starting address extraction pipeline...")
    start = time.time()

    conn = get_conn()
    try:
        setup_table(conn)
        extract_from_transactions(conn)
        extract_from_events(conn)
        mark_contracts(conn)
        print_summary(conn)
    finally:
        conn.close()

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
