#!/usr/bin/env python3
"""
FAST TRANSACTION BACKFILL via Blockscout REST API v2

The old approach (fetch_missing_transactions.py) made 2 RPC calls per tx hash
at ~0.25 tx/s → 41-day ETA. This script uses Blockscout's REST API to fetch
all transactions for a block in ONE call, which is 50-100x faster.

Strategy:
  1. Find blocks that have raw_logs but no transactions (chunked query)
  2. For each block, GET /api/v2/blocks/{number}/transactions
  3. Parse and insert all transactions in bulk
  4. Checkpoint progress for crash resilience

Run:  python3 -u scripts/fast_tx_backfill.py
      python3 -u scripts/fast_tx_backfill.py --dry-run     # just count
      python3 -u scripts/fast_tx_backfill.py --workers 3    # parallel fetches
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

import requests
from src.loaders.neon import NeonLoader

# ── CONFIG ──────────────────────────────────────────────────────
BLOCKSCOUT_API = "https://explorer.incentiv.io/api/v2"
CHECKPOINT_FILE = Path("data/fast_tx_backfill_checkpoint.json")
RATE_LIMIT_DELAY = 0.2  # seconds between API calls
COMMIT_BATCH = 200       # blocks per DB commit

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json",
})


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Just count missing blocks")
    p.add_argument("--workers", type=int, default=1, help="Parallel workers (be gentle)")
    p.add_argument("--max-blocks", type=int, default=0, help="Limit blocks to process")
    p.add_argument("--resume", action="store_true", default=True, help="Resume from checkpoint")
    return p.parse_args()


def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"blocks_done": [], "last_block": 0, "total_txs_inserted": 0}


def save_checkpoint(cp: dict):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    cp["updated_at"] = datetime.now(timezone.utc).isoformat()
    CHECKPOINT_FILE.write_text(json.dumps(cp))


def find_blocks_missing_txs(neon: NeonLoader, chunk_size: int = 50000) -> List[int]:
    """Find blocks that have raw_logs but no transactions."""
    print("Finding blocks with logs but no transactions...", flush=True)

    result = neon.query("SELECT MIN(block_number), MAX(block_number) FROM raw_logs")
    min_blk, max_blk = result[0]
    print(f"  Raw logs span blocks {min_blk:,} to {max_blk:,}", flush=True)

    missing_blocks: Set[int] = set()

    for chunk_start in range(min_blk, max_blk + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, max_blk)
        try:
            rows = neon.query(f"""
                SELECT DISTINCT rl.block_number
                FROM raw_logs rl
                LEFT JOIN transactions tx ON tx.block_number = rl.block_number
                WHERE rl.block_number >= {chunk_start}
                  AND rl.block_number <= {chunk_end}
                  AND tx.block_number IS NULL
            """)
            chunk_missing = {r[0] for r in rows}
            missing_blocks.update(chunk_missing)
            if chunk_missing:
                pct = (chunk_end - min_blk) / max(1, max_blk - min_blk) * 100
                print(f"  Chunk {chunk_start:,}-{chunk_end:,} ({pct:.0f}%): "
                      f"{len(chunk_missing):,} missing blocks "
                      f"(total: {len(missing_blocks):,})", flush=True)
        except Exception as e:
            print(f"  Error scanning chunk {chunk_start}-{chunk_end}: {e}", flush=True)
            try:
                neon.reconnect()
            except Exception:
                pass
            continue

    return sorted(missing_blocks)


def fetch_block_transactions(block_number: int) -> List[Dict[str, Any]]:
    """Fetch all transactions for a block via Blockscout REST API v2."""
    url = f"{BLOCKSCOUT_API}/blocks/{block_number}/transactions"
    txs = []
    next_page = None

    while True:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            params = next_page if next_page else {}
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            txs.extend(items)

            next_params = data.get("next_page_params")
            if not next_params:
                break
            next_page = next_params
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                break  # Block doesn't exist in Blockscout
            raise
        except Exception as e:
            print(f"    Error fetching block {block_number}: {e}", flush=True)
            break

    return txs


def parse_blockscout_tx(tx: Dict[str, Any], block_number: int) -> Optional[Dict[str, Any]]:
    """Parse a Blockscout REST API v2 transaction into our schema."""
    try:
        tx_hash = tx.get("hash", "")
        if not tx_hash:
            return None

        # Parse timestamp
        ts_raw = tx.get("timestamp") or tx.get("block", {}).get("timestamp")
        block_ts = None
        if ts_raw:
            try:
                block_ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                pass

        from_addr = ""
        if isinstance(tx.get("from"), dict):
            from_addr = tx["from"].get("hash", "")
        elif isinstance(tx.get("from"), str):
            from_addr = tx["from"]

        to_addr = ""
        if isinstance(tx.get("to"), dict):
            to_addr = tx["to"].get("hash", "")
        elif isinstance(tx.get("to"), str):
            to_addr = tx.get("to", "")

        return {
            "hash": tx_hash.lower(),
            "block_number": block_number,
            "from_address": from_addr.lower() if from_addr else None,
            "to_address": to_addr.lower() if to_addr else None,
            "value": tx.get("value", "0"),
            "gas_price": tx.get("gas_price", "0"),
            "gas": int(tx.get("gas_limit", 0) or 0),
            "gas_used": int(tx.get("gas_used", 0) or 0),
            "input": None,  # Skip input data to save space
            "input_data": None,
            "status": "success" if tx.get("status") == "ok" else tx.get("status", ""),
            "nonce": str(tx.get("nonce", "")),
            "transaction_index": tx.get("position"),
            "block_hash": tx.get("block_hash", tx.get("block", {}).get("hash", "")),
            "block_timestamp": block_ts,
            "timestamp": block_ts,
            "chain": "incentiv",
            "extracted_at": datetime.now(timezone.utc),
        }
    except Exception as e:
        print(f"    Parse error: {e}", flush=True)
        return None


def insert_transactions(neon: NeonLoader, tx_rows: List[Dict]) -> int:
    """Bulk insert transactions."""
    if not tx_rows:
        return 0

    import pandas as pd
    df = pd.DataFrame(tx_rows)

    cols = ["hash", "block_number", "from_address", "to_address", "value",
            "gas_price", "gas", "gas_used", "input", "input_data", "status",
            "nonce", "transaction_index", "block_hash", "block_timestamp",
            "timestamp", "chain", "extracted_at"]
    df = df[[c for c in cols if c in df.columns]]

    try:
        inserted = neon.copy_dataframe("transactions", df)
        return inserted
    except Exception as e:
        print(f"    Insert error: {e}", flush=True)
        try:
            neon.reconnect()
        except Exception:
            pass
        return 0


def main():
    args = parse_args()

    print("=" * 60, flush=True)
    print("FAST TRANSACTION BACKFILL (Blockscout REST API v2)", flush=True)
    print("=" * 60, flush=True)

    neon = NeonLoader()

    # Current counts
    for table in ["blocks", "transactions", "raw_logs", "decoded_events"]:
        count = neon.query(f"SELECT COUNT(*) FROM {table}")[0][0]
        print(f"  {table}: {count:,}", flush=True)
    print(flush=True)

    # Find missing blocks
    missing_blocks = find_blocks_missing_txs(neon)
    print(f"\nTotal blocks needing transactions: {len(missing_blocks):,}", flush=True)

    if args.dry_run:
        print("DRY RUN — exiting.", flush=True)
        neon.close()
        return

    if not missing_blocks:
        print("No missing blocks! All raw_logs have matching transactions.", flush=True)
        neon.close()
        return

    # Load checkpoint
    cp = load_checkpoint()
    done_set = set(cp.get("blocks_done", []))
    remaining = [b for b in missing_blocks if b not in done_set]
    print(f"Already done: {len(done_set):,}, Remaining: {len(remaining):,}", flush=True)

    if args.max_blocks:
        remaining = remaining[:args.max_blocks]
        print(f"Limited to {args.max_blocks} blocks", flush=True)

    total_inserted = cp.get("total_txs_inserted", 0)
    start_time = time.time()
    batch_txs = []
    blocks_this_batch = []

    for i, block_num in enumerate(remaining):
        try:
            txs_raw = fetch_block_transactions(block_num)

            tx_rows = []
            for tx in txs_raw:
                parsed = parse_blockscout_tx(tx, block_num)
                if parsed:
                    tx_rows.append(parsed)

            batch_txs.extend(tx_rows)
            blocks_this_batch.append(block_num)

            # Commit in batches
            if len(blocks_this_batch) >= COMMIT_BATCH or i == len(remaining) - 1:
                if batch_txs:
                    inserted = insert_transactions(neon, batch_txs)
                    total_inserted += inserted

                # Update checkpoint
                done_set.update(blocks_this_batch)
                cp["blocks_done"] = list(done_set)[-10000:]  # Keep last 10K for file size
                cp["last_block"] = block_num
                cp["total_txs_inserted"] = total_inserted
                save_checkpoint(cp)

                elapsed = time.time() - start_time
                rate = (i + 1) / max(1, elapsed)
                eta_min = (len(remaining) - i - 1) / max(0.01, rate) / 60
                print(f"  [{i+1:,}/{len(remaining):,}] "
                      f"Block {block_num:,} | "
                      f"Batch: {len(batch_txs)} txs | "
                      f"Total inserted: {total_inserted:,} | "
                      f"{rate:.1f} blk/s | "
                      f"ETA: {eta_min:.0f}m",
                      flush=True)

                batch_txs = []
                blocks_this_batch = []

        except Exception as e:
            print(f"  Error on block {block_num}: {e}", flush=True)
            try:
                neon.reconnect()
            except Exception:
                pass
            time.sleep(2)
            continue

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.0f}s", flush=True)
    print(f"Total transactions inserted: {total_inserted:,}", flush=True)

    # Final count
    tx_count = neon.query("SELECT COUNT(*) FROM transactions")[0][0]
    print(f"Transactions in DB: {tx_count:,}", flush=True)
    neon.close()


if __name__ == "__main__":
    main()
