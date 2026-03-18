#!/usr/bin/env python3
"""
BACKFILL RAW LOGS FOR ALREADY-INDEXED BLOCKS (v2 — crash-resilient)

The block + transaction backfill (via Blockscout API) inserted ~1.3M blocks
and their transactions into the DB, but completely skipped event logs.
Without logs, there are no raw_logs → no decoded_events → dashboard stays stale.

This script fixes that by using eth_getLogs via RPC to fetch ALL event logs
for the backfilled block ranges, then inserts them into raw_logs.

v2 improvements:
  - Auto-reconnects on Neon connection drops (no more crashes at 42%)
  - Retries failed ranges up to 3 times with backoff
  - Saves progress to a checkpoint file for resumability
  - Handles psycopg2.OperationalError gracefully

After this script runs, you should run:
  python3 scripts/redecode_entrypoint.py
to decode the new logs into decoded_events (UserOps, Swaps, Transfers, Bridges).

Usage:
  python3 scripts/backfill_logs.py                    # Process all blocks missing logs
  python3 scripts/backfill_logs.py --max-blocks 10000 # Limit to first 10K blocks
  python3 scripts/backfill_logs.py --from-block 318410 --to-block 1026424  # Specific range
  python3 scripts/backfill_logs.py --dry-run           # Show what would be processed
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.extractors.blockscout import BlockscoutExtractor
from src.transformers.raw_logs import normalize_raw_logs
from src.loaders.neon import NeonLoader


# ── CONFIG ────────────────────────────────────────────────────────
RPC_URL = os.getenv("INCENTIV_BLOCKSCOUT_RPC_URL", "https://explorer.incentiv.io/api/eth-rpc")
BATCH_SIZE = 50          # Blocks per eth_getLogs call (keep small to avoid 413)
RATE_LIMIT = 5.0         # RPC requests per second
MAX_RETRIES = 3          # Retries per range on DB errors
CHECKPOINT_FILE = Path("data/backfill_logs_checkpoint.json")


def save_checkpoint(blocks_processed: int, total_logs: int, total_inserted: int, errors: int, last_block: int):
    """Save progress so we can report status even if the script restarts."""
    CHECKPOINT_FILE.parent.mkdir(exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({
        "updated_at": datetime.utcnow().isoformat(),
        "blocks_processed": blocks_processed,
        "total_logs_fetched": total_logs,
        "total_logs_inserted": total_inserted,
        "errors": errors,
        "last_block_processed": last_block,
    }, indent=2))


def find_blocks_missing_logs(neon: NeonLoader, from_block: int = 0, to_block: int = 0) -> List[int]:
    """
    Find blocks in our DB that have NO raw_logs entries.
    These are the blocks we need to fetch logs for.
    """
    print("\n  Finding blocks with no raw_logs...")

    where_clause = ""
    if from_block > 0:
        where_clause += f" AND b.number >= {from_block}"
    if to_block > 0:
        where_clause += f" AND b.number <= {to_block}"

    rows = neon.query(f"""
        SELECT b.number
        FROM blocks b
        LEFT JOIN (
            SELECT DISTINCT block_number FROM raw_logs
        ) r ON b.number = r.block_number
        WHERE r.block_number IS NULL
        {where_clause}
        ORDER BY b.number
    """)

    block_nums = [int(r[0]) for r in rows]
    print(f"  Found {len(block_nums):,} blocks with no raw_logs")
    return block_nums


def group_into_ranges(block_nums: List[int], max_range: int = 500) -> List[Tuple[int, int]]:
    """
    Group a sorted list of block numbers into contiguous ranges.
    Splits ranges larger than max_range to keep RPC calls manageable.
    """
    if not block_nums:
        return []

    ranges = []
    start = block_nums[0]
    end = block_nums[0]

    for num in block_nums[1:]:
        if num == end + 1 and (num - start) < max_range:
            end = num
        else:
            ranges.append((start, end))
            start = num
            end = num

    ranges.append((start, end))
    return ranges


def get_block_timestamps(neon: NeonLoader, block_numbers: List[int]) -> Dict[int, str]:
    """Fetch block timestamps from DB for enriching logs."""
    if not block_numbers:
        return {}

    timestamps = {}
    chunk_size = 1000
    for i in range(0, len(block_numbers), chunk_size):
        chunk = block_numbers[i:i + chunk_size]
        placeholders = ",".join(str(b) for b in chunk)
        rows = neon.query(f"""
            SELECT number, timestamp
            FROM blocks
            WHERE number IN ({placeholders})
        """)
        for num, ts in rows:
            timestamps[int(num)] = ts

    return timestamps


def enrich_logs_with_timestamps(logs: List[Dict], timestamps: Dict[int, Any]) -> List[Dict]:
    """Add block_timestamp to each log from our blocks table."""
    for log in logs:
        block_num = int(log.get("blockNumber", "0x0"), 16)
        ts = timestamps.get(block_num)
        log["block_timestamp"] = ts
    return logs


def process_range_with_retry(
    neon: NeonLoader,
    extractor: BlockscoutExtractor,
    range_start: int,
    range_end: int,
) -> Tuple[int, int, bool]:
    """
    Process a single block range with retry logic for DB connection drops.
    Returns (logs_fetched, logs_inserted, success).
    """
    range_size = range_end - range_start + 1

    for attempt in range(MAX_RETRIES):
        try:
            # Get timestamps
            block_nums_in_range = list(range(range_start, range_end + 1))
            timestamps = get_block_timestamps(neon, block_nums_in_range)

            # Fetch logs via eth_getLogs
            logs = extractor.get_all_logs(range_start, range_end)

            if not logs:
                return 0, 0, True  # No logs = success (empty blocks)

            # Enrich with timestamps
            logs = enrich_logs_with_timestamps(logs, timestamps)

            # Normalize to DataFrame
            df = normalize_raw_logs(logs, chain="incentiv")

            if df.empty:
                return len(logs), 0, True

            # Insert into raw_logs
            inserted = neon.copy_dataframe("raw_logs", df)
            return len(logs), inserted, True

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # Connection dropped — reconnect and retry
            wait = 5 * (attempt + 1)
            print(f"  DB connection lost at blocks {range_start}-{range_end} "
                  f"(attempt {attempt+1}/{MAX_RETRIES}): {e}")
            print(f"  Reconnecting in {wait}s...")
            time.sleep(wait)
            neon.reconnect()

        except Exception as e:
            # Non-connection error — log and skip
            print(f"  ERROR at blocks {range_start}-{range_end}: {e}")
            try:
                neon.conn.rollback()
            except Exception:
                pass
            return 0, 0, False

    # All retries exhausted
    print(f"  FAILED blocks {range_start}-{range_end} after {MAX_RETRIES} retries. Skipping.")
    return 0, 0, False


def backfill_logs(
    neon: NeonLoader,
    extractor: BlockscoutExtractor,
    from_block: int = 0,
    to_block: int = 0,
    max_blocks: int = 0,
    dry_run: bool = False,
):
    """Main backfill loop with crash resilience."""
    print("=" * 60)
    print("  RAW LOGS BACKFILL (v2 — crash-resilient)")
    print("=" * 60)

    # Step 1: Find blocks missing logs
    missing_blocks = find_blocks_missing_logs(neon, from_block, to_block)

    if max_blocks > 0:
        missing_blocks = missing_blocks[:max_blocks]
        print(f"  Limited to first {max_blocks:,} blocks")

    if not missing_blocks:
        print("  No blocks missing logs. Nothing to do!")
        return

    # Step 2: Group into contiguous ranges
    ranges = group_into_ranges(missing_blocks, max_range=BATCH_SIZE * 10)
    total_blocks = len(missing_blocks)

    print(f"\n  Blocks to process: {total_blocks:,}")
    print(f"  Contiguous ranges: {len(ranges)}")
    print(f"  Block range: {missing_blocks[0]:,} to {missing_blocks[-1]:,}")

    if dry_run:
        print("\n  DRY RUN — showing first 10 ranges:")
        for start, end in ranges[:10]:
            print(f"    {start:>10,} → {end:>10,}  ({end - start + 1:,} blocks)")
        if len(ranges) > 10:
            print(f"    ... and {len(ranges) - 10} more ranges")
        return

    # Step 3: Fetch logs for each range
    total_logs = 0
    total_inserted = 0
    blocks_processed = 0
    errors = 0
    start_time = time.time()

    for range_idx, (range_start, range_end) in enumerate(ranges):
        range_size = range_end - range_start + 1

        logs_fetched, logs_inserted, success = process_range_with_retry(
            neon, extractor, range_start, range_end
        )

        total_logs += logs_fetched
        total_inserted += logs_inserted
        blocks_processed += range_size
        if not success:
            errors += 1

        # Progress report every 10 ranges
        if (range_idx + 1) % 10 == 0 or blocks_processed >= total_blocks:
            elapsed = time.time() - start_time
            rate = blocks_processed / elapsed if elapsed > 0 else 0
            remaining = total_blocks - blocks_processed
            eta_s = remaining / rate if rate > 0 else 0
            eta_h = eta_s / 3600

            pct = blocks_processed / total_blocks * 100
            print(
                f"  [{blocks_processed:,}/{total_blocks:,}] {pct:.1f}% | "
                f"{rate:.0f} blk/s | "
                f"Logs: {total_logs:,} fetched, {total_inserted:,} inserted | "
                f"ETA: {eta_h:.1f}h | Errors: {errors}"
            )

            # Save checkpoint
            save_checkpoint(blocks_processed, total_logs, total_inserted, errors, range_end)

    elapsed = time.time() - start_time
    print(f"\n  {'=' * 50}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Blocks processed: {blocks_processed:,}")
    print(f"  Logs fetched:     {total_logs:,}")
    print(f"  Logs inserted:    {total_inserted:,}")
    print(f"  Errors:           {errors}")
    print(f"  {'=' * 50}")

    # Save final checkpoint
    save_checkpoint(blocks_processed, total_logs, total_inserted, errors, ranges[-1][1] if ranges else 0)

    if total_inserted > 0:
        print(f"\n  NEXT STEP: Run the event decoder to populate decoded_events:")
        print(f"    python3 scripts/redecode_entrypoint.py")


def main():
    parser = argparse.ArgumentParser(description="Backfill raw_logs for blocks missing log data (v2)")
    parser.add_argument("--from-block", type=int, default=0, help="Start block number")
    parser.add_argument("--to-block", type=int, default=0, help="End block number")
    parser.add_argument("--max-blocks", type=int, default=0, help="Max blocks to process (0=all)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Blocks per RPC call (default: {BATCH_SIZE})")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without doing it")
    args = parser.parse_args()

    neon = NeonLoader()

    # Create extractor with conservative settings
    extractor = BlockscoutExtractor(
        base_url="https://explorer.incentiv.io/api",
        rpc_url=RPC_URL,
        confirmations=0,
        batch_size=args.batch_size,
        rate_limit_per_second=RATE_LIMIT,
    )

    try:
        backfill_logs(
            neon=neon,
            extractor=extractor,
            from_block=args.from_block,
            to_block=args.to_block,
            max_blocks=args.max_blocks,
            dry_run=args.dry_run,
        )
    finally:
        neon.close()


if __name__ == "__main__":
    main()
