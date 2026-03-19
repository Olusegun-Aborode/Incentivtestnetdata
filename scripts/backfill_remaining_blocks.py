#!/usr/bin/env python3
"""
Backfill missing blocks from the Incentiv RPC.

Finds gaps between our indexed blocks and the chain head,
then fetches missing blocks + transactions in batches.
"""

import os
import sys
import time
import json
import requests
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
RPC_URL = os.environ.get("RPC_URL", "https://incentiv.rpc.caldera.xyz/http")

if not DATABASE_URL:
    print("ERROR: Set NEON_DATABASE_URL or DATABASE_URL environment variable")
    sys.exit(1)

BATCH_SIZE = 100  # blocks per batch
RPC_BATCH_SIZE = 20  # JSON-RPC batch calls

def get_conn():
    return psycopg2.connect(DATABASE_URL, options="-c statement_timeout=300000")

def rpc_call(method, params=None):
    """Single JSON-RPC call."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params or [], "id": 1}
    r = requests.post(RPC_URL, json=payload, timeout=30)
    result = r.json()
    if "error" in result:
        raise Exception(f"RPC error: {result['error']}")
    return result.get("result")

def rpc_batch(calls):
    """Batch JSON-RPC call."""
    payload = [{"jsonrpc": "2.0", "method": m, "params": p, "id": i}
               for i, (m, p) in enumerate(calls)]
    r = requests.post(RPC_URL, json=payload, timeout=60)
    results = r.json()
    return sorted(results, key=lambda x: x["id"])

def get_chain_head():
    """Get latest block number from chain."""
    result = rpc_call("eth_blockNumber")
    return int(result, 16)

def find_missing_blocks(conn, start=0, end=None):
    """Find block numbers we don't have indexed."""
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(number), MAX(number), COUNT(*) FROM blocks")
        db_min, db_max, db_count = cur.fetchone()
        print(f"DB: blocks {db_min:,} to {db_max:,} ({db_count:,} total)")

        if end is None:
            end = get_chain_head()
        print(f"Chain head: {end:,}")
        print(f"Expected: {end - start + 1:,} blocks, Missing: ~{(end - start + 1) - db_count:,}")

        # Find gaps using generate_series
        cur.execute("""
            SELECT g.n
            FROM generate_series(%s::bigint, %s::bigint) g(n)
            LEFT JOIN blocks b ON b.number = g.n
            WHERE b.number IS NULL
            ORDER BY g.n
        """, (max(start, db_max - 1000), end))  # Start from near the top to avoid huge series
        missing = [row[0] for row in cur.fetchall()]
        print(f"Found {len(missing):,} missing blocks in range {max(start, db_max - 1000):,} to {end:,}")
        return missing

def fetch_and_store_blocks(conn, block_numbers):
    """Fetch blocks from RPC and store in DB."""
    total = len(block_numbers)
    stored = 0

    for i in range(0, total, RPC_BATCH_SIZE):
        batch = block_numbers[i:i + RPC_BATCH_SIZE]
        calls = [("eth_getBlockByNumber", [hex(n), True]) for n in batch]

        try:
            results = rpc_batch(calls)
        except Exception as e:
            print(f"  RPC error at batch {i}: {e}")
            time.sleep(2)
            continue

        block_rows = []
        tx_rows = []

        for res in results:
            block = res.get("result")
            if not block:
                continue

            number = int(block["number"], 16)
            timestamp = int(block["timestamp"], 16)
            gas_used = int(block.get("gasUsed", "0x0"), 16)
            gas_limit = int(block.get("gasLimit", "0x0"), 16)
            tx_count = len(block.get("transactions", []))

            block_rows.append((
                number,
                block.get("hash", ""),
                block.get("parentHash", ""),
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp)),
                gas_used,
                gas_limit,
                tx_count,
                block.get("miner", ""),
            ))

            for tx in block.get("transactions", []):
                if isinstance(tx, str):
                    continue  # Skip if just hash
                tx_rows.append((
                    tx.get("hash", ""),
                    int(tx.get("blockNumber", "0x0"), 16),
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp)),
                    tx.get("from", ""),
                    tx.get("to", ""),
                    str(int(tx.get("value", "0x0"), 16)),
                    str(int(tx.get("gas", "0x0"), 16)),
                    str(int(tx.get("gasPrice", "0x0"), 16)),
                    1,  # status (assume success, we'd need receipt for actual)
                    tx.get("input", "0x")[:200],  # Truncate input data
                ))

        if block_rows:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner)
                    VALUES %s
                    ON CONFLICT (number) DO NOTHING
                """, block_rows)

                if tx_rows:
                    execute_values(cur, """
                        INSERT INTO transactions (hash, block_number, block_timestamp, from_address, to_address, value, gas_used, gas_price, status, input)
                        VALUES %s
                        ON CONFLICT (hash) DO NOTHING
                    """, tx_rows)

            conn.commit()
            stored += len(block_rows)

        if (i + RPC_BATCH_SIZE) % 200 == 0 or i + RPC_BATCH_SIZE >= total:
            print(f"  Progress: {min(i + RPC_BATCH_SIZE, total):,}/{total:,} blocks ({stored:,} stored)")

        time.sleep(0.1)  # Rate limit

    return stored

def main():
    print("=== Block Backfill Pipeline ===")
    start = time.time()

    conn = get_conn()
    try:
        missing = find_missing_blocks(conn)

        if not missing:
            print("No missing blocks found!")
            return

        print(f"\nBackfilling {len(missing):,} blocks...")
        stored = fetch_and_store_blocks(conn, missing)
        print(f"\nStored {stored:,} blocks")

        # Verify
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM blocks")
            total = cur.fetchone()[0]
            print(f"Total blocks in DB: {total:,}")

    finally:
        conn.close()

    elapsed = time.time() - start
    print(f"Completed in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
