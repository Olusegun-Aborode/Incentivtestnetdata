#!/usr/bin/env python3
"""
Find the block number closest to a given date using binary search on the RPC.

Usage:
    python scripts/find_block_by_date.py                          # Find Dec 1 2025 block
    python scripts/find_block_by_date.py --date "2025-12-01"      # Custom date
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.config import load_yaml
from src.extractors.blockscout import BlockscoutExtractor


def get_block_timestamp(extractor: BlockscoutExtractor, block_num: int) -> datetime:
    """Get the timestamp for a specific block."""
    blocks = extractor.get_blocks_by_number([block_num])
    if block_num not in blocks:
        return None
    ts_hex = blocks[block_num].get("timestamp", "0x0")
    return datetime.utcfromtimestamp(int(ts_hex, 16))


def find_block_for_date(extractor: BlockscoutExtractor, target_date: datetime) -> int:
    """Binary search for the block closest to target_date."""
    low = 0
    high = extractor.get_latest_block_number()

    print(f"  Latest block: {high:,}")
    print(f"  Target date:  {target_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Searching...")

    best_block = low
    iterations = 0

    while low <= high and iterations < 50:
        mid = (low + high) // 2
        ts = get_block_timestamp(extractor, mid)
        iterations += 1

        if ts is None:
            # Block doesn't exist, try nearby
            mid += 1
            ts = get_block_timestamp(extractor, mid)
            if ts is None:
                high = mid - 2
                continue

        if ts < target_date:
            best_block = mid
            low = mid + 1
        else:
            high = mid - 1

        if iterations % 5 == 0:
            print(f"    block {mid:,} = {ts.strftime('%Y-%m-%d %H:%M')} (iteration {iterations})")

    # Refine: check a few blocks around best_block
    final_ts = get_block_timestamp(extractor, best_block)
    print(f"\n  Result: block {best_block:,} = {final_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return best_block


def main():
    parser = argparse.ArgumentParser(description="Find block number for a given date")
    parser.add_argument("--date", default="2025-12-01",
                        help="Target date in YYYY-MM-DD format (default: 2025-12-01)")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d")

    chains = load_yaml("config/chains.yaml")
    cfg = chains["incentiv"]

    extractor = BlockscoutExtractor(
        base_url=cfg["blockscout_base_url"],
        rpc_url=cfg["blockscout_rpc_url"],
        confirmations=int(cfg["confirmations"]),
        batch_size=int(cfg["batch_size"]),
        rate_limit_per_second=float(cfg["rate_limit_per_second"]),
    )

    print(f"Finding block for {args.date}...")
    block = find_block_for_date(extractor, target_date)

    print(f"\n{'='*50}")
    print(f"To backfill from {args.date} to now, run:")
    print(f"")
    print(f"  python -m src.pipeline --chain incentiv --all-activity --neon --from-block {block}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
