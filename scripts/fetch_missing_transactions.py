#!/usr/bin/env python3
"""
FETCH REAL TRANSACTION DATA FOR ORPHANED RAW LOGS

The log backfill fetched 12.4M raw_logs via eth_getLogs, but ~860K of those
logs reference transaction hashes that don't exist in our `transactions` table.
The `decoded_events` table has a FOREIGN KEY on transaction_hash → transactions(hash),
so decoding those logs fails with FK violations.

This script fetches the REAL transaction data from the chain (via RPC receipts
or Blockscout API), NOT dummy/placeholder data. Every row inserted is a real
on-chain transaction with accurate from/to addresses, gas, value, and status.

Strategy:
  1. Find distinct transaction hashes in raw_logs that are NOT in transactions
  2. Fetch real transaction data via eth_getTransactionReceipt (batched)
  3. Also fetch from_address via eth_getTransactionByHash (receipts lack 'from')
  4. Insert into transactions table with ON CONFLICT DO NOTHING

Usage:
  python3 scripts/fetch_missing_transactions.py                  # Fetch all missing
  python3 scripts/fetch_missing_transactions.py --max-txs 10000  # Limit for testing
  python3 scripts/fetch_missing_transactions.py --dry-run        # Just count missing
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.utils.http import HttpClient
from src.loaders.neon import NeonLoader


# ── CONFIG ────────────────────────────────────────────────────────
RPC_URL = os.getenv("INCENTIV_BLOCKSCOUT_RPC_URL", "https://explorer.incentiv.io/api/eth-rpc")
BLOCKSCOUT_API = "https://explorer.incentiv.io/api/v2"
BATCH_SIZE = 10          # Tx hashes per RPC batch (Incentiv RPC is strict on payload size)
COMMIT_EVERY = 500       # Commit to DB every N transactions
RATE_LIMIT = 5.0


def find_missing_tx_hashes(neon: NeonLoader, limit: int = 0) -> List[str]:
    """Find transaction hashes in raw_logs that don't exist in transactions by chunking."""
    print("\n  Finding orphaned transaction hashes in raw_logs (chunked)...", flush=True)

    block_query = "SELECT MIN(block_number), MAX(block_number) FROM raw_logs"
    min_blk, max_blk = neon.query(block_query)[0]
    
    if min_blk is None:
        return []

    hashes = set()
    chunk_size = 50000
    
    for chunk_start in range(min_blk, max_blk + 1, chunk_size):
        chunk_end = chunk_start + chunk_size - 1
        
        limit_clause = f"LIMIT {limit - len(hashes)}" if limit > 0 else ""
        
        q = f"""
            SELECT DISTINCT r.transaction_hash
            FROM raw_logs r
            LEFT JOIN transactions t ON r.transaction_hash = t.hash
            WHERE r.block_number >= {chunk_start} AND r.block_number <= {chunk_end}
              AND t.hash IS NULL
            {limit_clause}
        """
        
        rows = neon.query(q)
        for r in rows:
            hashes.add(r[0])
            
        print(f"  Scanned blocks {chunk_start}-{chunk_end}... Found {len(hashes):,} missing so far.", flush=True)
            
        if limit > 0 and len(hashes) >= limit:
            break

    # Convert to list and slice strictly to limit if provided
    final_list = list(hashes)
    if limit > 0:
        final_list = final_list[:limit]
        
    print(f"  Total missing transactions to fetch: {len(final_list):,}", flush=True)
    return final_list


def fetch_tx_receipt_rpc(rpc_client: HttpClient, tx_hash: str) -> Optional[Dict]:
    """Fetch a single transaction receipt via RPC."""
    try:
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
        }
        response = rpc_client.post("", payload)
        return response.get("result")
    except Exception:
        return None


def fetch_tx_data_rpc(rpc_client: HttpClient, tx_hash: str) -> Optional[Dict]:
    """Fetch transaction data (includes from_address) via RPC."""
    try:
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
        }
        response = rpc_client.post("", payload)
        return response.get("result")
    except Exception:
        return None


def fetch_batch_receipts(rpc_client: HttpClient, tx_hashes: List[str]) -> Dict[str, Dict]:
    """Fetch a batch of transaction receipts via RPC batch call."""
    payloads = [
        {"id": i, "jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [h]}
        for i, h in enumerate(tx_hashes)
    ]
    try:
        responses = rpc_client.post_batch("", payloads)
        results = {}
        for resp in responses:
            receipt = resp.get("result")
            if receipt and receipt.get("transactionHash"):
                results[receipt["transactionHash"].lower()] = receipt
        return results
    except Exception:
        # Fall back to individual requests
        results = {}
        for h in tx_hashes:
            receipt = fetch_tx_receipt_rpc(rpc_client, h)
            if receipt:
                results[h.lower()] = receipt
        return results


def fetch_batch_txdata(rpc_client: HttpClient, tx_hashes: List[str]) -> Dict[str, Dict]:
    """Fetch a batch of transaction data via RPC batch call."""
    payloads = [
        {"id": i, "jsonrpc": "2.0", "method": "eth_getTransactionByHash", "params": [h]}
        for i, h in enumerate(tx_hashes)
    ]
    try:
        responses = rpc_client.post_batch("", payloads)
        results = {}
        for resp in responses:
            tx = resp.get("result")
            if tx and tx.get("hash"):
                results[tx["hash"].lower()] = tx
        return results
    except Exception:
        results = {}
        for h in tx_hashes:
            tx = fetch_tx_data_rpc(rpc_client, h)
            if tx:
                results[h.lower()] = tx
        return results


def insert_transactions(neon: NeonLoader, tx_rows: List[Dict]) -> int:
    """Insert transaction rows into Neon using parameterized queries."""
    if not tx_rows:
        return 0

    INSERT_SQL = """
        INSERT INTO transactions (hash, block_number, from_address, to_address,
                                   value, gas_price, gas, gas_used, status,
                                   nonce, transaction_index, block_hash,
                                   block_timestamp, chain, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'incentiv', NOW())
        ON CONFLICT (hash) DO NOTHING
    """

    inserted = 0
    cur = neon.conn.cursor()
    try:
        for row in tx_rows:
            cur.execute(INSERT_SQL, (
                row["hash"],
                row["block_number"],
                row.get("from_address"),
                row.get("to_address"),
                row.get("value", "0"),
                row.get("gas_price", "0"),
                row.get("gas", 0),
                row.get("gas_used", 0),
                row.get("status", ""),
                row.get("nonce", "0"),
                row.get("transaction_index", 0),
                row.get("block_hash", ""),
                row.get("block_timestamp"),
            ))
            inserted += 1
        neon.conn.commit()
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        print(f"  DB connection error during insert: {e}")
        neon.reconnect()
        return 0
    except Exception as e:
        print(f"  Insert error: {e}")
        try:
            neon.conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        cur.close()

    return inserted


def get_block_timestamp(neon: NeonLoader, block_number: int) -> Optional[str]:
    """Get timestamp for a block from our DB."""
    try:
        rows = neon.query(f"SELECT timestamp FROM blocks WHERE number = {block_number}")
        return rows[0][0] if rows else None
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Fetch real transaction data for orphaned logs")
    parser.add_argument("--max-txs", type=int, default=0, help="Max transactions to fetch (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Just count missing, don't fetch")
    args = parser.parse_args()

    neon = NeonLoader()
    rpc_client = HttpClient(RPC_URL, rate_limit_per_second=RATE_LIMIT)

    print("=" * 60)
    print("  FETCH MISSING TRANSACTIONS (REAL DATA)")
    print("=" * 60)

    # Step 1: Find missing hashes
    missing_hashes = find_missing_tx_hashes(neon, limit=args.max_txs)

    if not missing_hashes:
        print("  No missing transactions. All raw_logs have matching transactions!")
        neon.close()
        return

    if args.dry_run:
        print(f"\n  DRY RUN: {len(missing_hashes):,} transactions would be fetched.")
        print(f"  First 10: {missing_hashes[:10]}")
        neon.close()
        return

    # Step 2: Fetch in batches
    total = len(missing_hashes)
    fetched = 0
    inserted = 0
    errors = 0
    start_time = time.time()

    # Cache block timestamps to avoid repeated DB queries
    ts_cache: Dict[int, Any] = {}

    for i in range(0, total, BATCH_SIZE):
        batch = missing_hashes[i:i + BATCH_SIZE]

        # Fetch receipts (has gas_used, status, logs, but NOT from_address)
        receipts = fetch_batch_receipts(rpc_client, batch)

        # Fetch tx data (has from_address, value, gas_price, nonce)
        txdata = fetch_batch_txdata(rpc_client, batch)

        # Merge and build rows
        tx_rows = []
        for tx_hash in batch:
            h = tx_hash.lower()
            receipt = receipts.get(h)
            txd = txdata.get(h)

            if not receipt and not txd:
                errors += 1
                continue

            block_num = int((receipt or txd).get("blockNumber", "0x0"), 16)

            # Get block timestamp (from cache or DB)
            if block_num not in ts_cache:
                ts_cache[block_num] = get_block_timestamp(neon, block_num)
            block_ts = ts_cache.get(block_num)

            row = {
                "hash": h,
                "block_number": block_num,
                "from_address": (txd.get("from", "") or "").lower() if txd else "",
                "to_address": (
                    (receipt or {}).get("to", "") or
                    (txd or {}).get("to", "") or ""
                ).lower(),
                "value": str(int(txd.get("value", "0x0"), 16)) if txd and txd.get("value") else "0",
                "gas_price": str(int(txd.get("gasPrice", "0x0"), 16)) if txd and txd.get("gasPrice") else "0",
                "gas": int(txd.get("gas", "0x0"), 16) if txd and txd.get("gas") else 0,
                "gas_used": int(receipt.get("gasUsed", "0x0"), 16) if receipt and receipt.get("gasUsed") else 0,
                "status": "ok" if receipt and receipt.get("status") == "0x1" else "error",
                "nonce": str(int(txd.get("nonce", "0x0"), 16)) if txd and txd.get("nonce") else "0",
                "transaction_index": int(
                    (receipt or txd or {}).get("transactionIndex", "0x0"), 16
                ),
                "block_hash": (receipt or txd or {}).get("blockHash", ""),
                "block_timestamp": block_ts,
            }
            tx_rows.append(row)

        # Insert batch
        batch_inserted = insert_transactions(neon, tx_rows)
        inserted += batch_inserted
        fetched += len(batch)

        # Progress
        if fetched % (BATCH_SIZE * 10) == 0 or fetched >= total:
            elapsed = time.time() - start_time
            rate = fetched / elapsed if elapsed > 0 else 0
            remaining = total - fetched
            eta_s = remaining / rate if rate > 0 else 0
            eta_m = eta_s / 60

            pct = fetched / total * 100
            print(
                f"  [{fetched:,}/{total:,}] {pct:.1f}% | "
                f"{rate:.0f} tx/s | "
                f"Inserted: {inserted:,} | Errors: {errors} | "
                f"ETA: {eta_m:.0f}m"
            )

    elapsed = time.time() - start_time
    print(f"\n  {'=' * 50}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Transactions fetched: {fetched:,}")
    print(f"  Transactions inserted: {inserted:,}")
    print(f"  Errors (not found on chain): {errors:,}")
    print(f"  {'=' * 50}")

    if inserted > 0:
        print(f"\n  NEXT STEP: Now decode the logs:")
        print(f"    python3 scripts/redecode_all.py")

    neon.close()


if __name__ == "__main__":
    main()
