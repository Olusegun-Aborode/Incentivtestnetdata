#!/usr/bin/env python3
"""
Backfill addresses from BlockScout REST API into Neon PostgreSQL.
Uses cursor-based pagination to fetch all addresses from:
  https://explorer.incentiv.io/api/v2/addresses

Creates and populates an 'addresses' table with:
  - hash (address)
  - coin_balance
  - transactions_count
  - is_contract
  - is_verified
  - name (contract name if available)

This closes the unique address count gap between our DB and the explorer.
"""

import os
import sys
import time
import json
import httpx
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env.neon'))

BLOCKSCOUT_BASE = "https://explorer.incentiv.io/api/v2"
DATABASE_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: No database URL found. Set NEON_DATABASE_URL or DATABASE_URL.")
    sys.exit(1)

# Strip channel_binding if present
if "channel_binding=require" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("?channel_binding=require", "").replace("&channel_binding=require", "")


def get_db_connection():
    """Get a database connection."""
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    conn.autocommit = False
    return conn


def create_addresses_table(conn):
    """Create the addresses table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS addresses (
                hash TEXT PRIMARY KEY,
                coin_balance TEXT,
                transactions_count INTEGER DEFAULT 0,
                is_contract BOOLEAN DEFAULT FALSE,
                is_verified BOOLEAN DEFAULT FALSE,
                name TEXT,
                proxy_type TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Add index for quick lookups
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_addresses_is_contract ON addresses(is_contract)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_addresses_tx_count ON addresses(transactions_count DESC)
        """)
    conn.commit()
    print("addresses table ready.")


def fetch_addresses_page(client, params=None):
    """Fetch a single page of addresses from BlockScout."""
    url = f"{BLOCKSCOUT_BASE}/addresses"
    for attempt in range(5):
        try:
            resp = client.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = min(2 ** attempt * 2, 30)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}, retrying...")
                time.sleep(2)
        except Exception as e:
            print(f"  Error: {e}, retrying...")
            time.sleep(2)
    return None


def insert_addresses(conn, addresses):
    """Bulk insert addresses into the database."""
    if not addresses:
        return 0

    rows = []
    for addr in addresses:
        tx_count_raw = addr.get('transactions_count', '0') or '0'
        try:
            tx_count = int(tx_count_raw)
        except (ValueError, TypeError):
            tx_count = 0
        rows.append((
            addr.get('hash', '').lower(),
            addr.get('coin_balance', '0') or '0',
            tx_count,
            bool(addr.get('is_contract', False)),
            bool(addr.get('is_verified', False)),
            addr.get('name') or None,
            addr.get('proxy_type') or None,
        ))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO addresses (hash, coin_balance, transactions_count, is_contract, is_verified, name, proxy_type)
               VALUES %s
               ON CONFLICT (hash) DO UPDATE SET
                 coin_balance = EXCLUDED.coin_balance,
                 transactions_count = EXCLUDED.transactions_count,
                 is_contract = EXCLUDED.is_contract,
                 is_verified = EXCLUDED.is_verified,
                 name = EXCLUDED.name,
                 proxy_type = EXCLUDED.proxy_type""",
            rows
        )
    conn.commit()
    return len(rows)


def main():
    print("=" * 60)
    print("BlockScout Address Backfill")
    print("=" * 60)

    conn = get_db_connection()
    create_addresses_table(conn)

    # Check current count
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM addresses")
        existing = cur.fetchone()[0]
    print(f"Existing addresses in DB: {existing}")

    client = httpx.Client(
        headers={"Accept": "application/json"},
        follow_redirects=True,
    )

    total_fetched = 0
    total_inserted = 0
    page = 0
    next_params = None

    try:
        while True:
            page += 1
            data = fetch_addresses_page(client, next_params)

            if not data or 'items' not in data:
                print(f"No more data at page {page}.")
                break

            items = data['items']
            if not items:
                print("Empty page, done.")
                break

            total_fetched += len(items)
            inserted = insert_addresses(conn, items)
            total_inserted += inserted

            if page % 10 == 0 or page <= 3:
                print(f"  Page {page}: fetched {len(items)} addresses | Total: {total_fetched} fetched, {total_inserted} upserted")

            # Check for next page
            next_page = data.get('next_page_params')
            if not next_page:
                print("No more pages (next_page_params is null).")
                break

            next_params = {
                'hash': next_page.get('hash'),
                'transactions_count': next_page.get('transactions_count'),
                'fetched_coin_balance': next_page.get('fetched_coin_balance'),
                'items_count': next_page.get('items_count'),
            }

            # Small delay to be nice to the API
            time.sleep(0.3)

    except KeyboardInterrupt:
        print(f"\nInterrupted. Saved {total_inserted} addresses so far.")
    finally:
        client.close()

    # Final count
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM addresses")
        final_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM addresses WHERE is_contract = true")
        contract_count = cur.fetchone()[0]

    print()
    print("=" * 60)
    print(f"DONE")
    print(f"  Pages processed: {page}")
    print(f"  Addresses fetched: {total_fetched}")
    print(f"  Addresses upserted: {total_inserted}")
    print(f"  Total in DB: {final_count}")
    print(f"  Contracts: {contract_count}")
    print(f"  EOAs: {final_count - contract_count}")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
