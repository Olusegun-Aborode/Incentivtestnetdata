#!/usr/bin/env python3
"""
Data quality validation for the Neon database.

Runs integrity checks, continuity analysis, FK validation,
and optional RPC sampling to verify data completeness.

Usage:
    python scripts/validate_neon.py                 # Full validation
    python scripts/validate_neon.py --quick         # Just counts and basic checks
    python scripts/validate_neon.py --rpc-sample 50 # Sample 50 random blocks from RPC
"""

import argparse
import os
import random
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.loaders.neon import NeonLoader


PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"

results = []


def check(name: str, status: str, detail: str = "") -> None:
    results.append((name, status, detail))
    icon = {"PASS": "+", "WARN": "~", "FAIL": "!"}[status]
    print(f"  [{icon}] {name}: {detail}" if detail else f"  [{icon}] {name}")


def validate_counts(neon: NeonLoader) -> None:
    """Check that all tables have data."""
    print("\n1. TABLE ROW COUNTS")
    counts = neon.get_table_counts()
    for table, count in counts.items():
        if count > 0:
            check(f"{table} count", PASS, f"{count:,} rows")
        elif count == 0:
            check(f"{table} count", WARN, "0 rows (empty)")
        else:
            check(f"{table} count", FAIL, "table missing or error")


def validate_block_continuity(neon: NeonLoader) -> None:
    """Check for gaps in block numbers."""
    print("\n2. BLOCK CONTINUITY")
    row = neon.query("""
        SELECT MIN(number) AS min_block,
               MAX(number) AS max_block,
               COUNT(*) AS total,
               MAX(number) - MIN(number) + 1 AS expected
        FROM blocks
    """)
    if not row or row[0][0] is None:
        check("block continuity", WARN, "no blocks in database")
        return

    min_b, max_b, total, expected = row[0]
    gap = expected - total

    check("block range", PASS, f"{min_b:,} to {max_b:,}")
    if gap == 0:
        check("block continuity", PASS, f"{total:,} blocks, no gaps")
    elif gap < 100:
        check("block continuity", WARN, f"{gap:,} missing blocks out of {expected:,}")
    else:
        check("block continuity", FAIL, f"{gap:,} missing blocks out of {expected:,} ({gap/expected*100:.1f}%)")

    # Find specific gap ranges (first 5)
    gaps = neon.query("""
        SELECT number + 1 AS gap_start,
               next_number - 1 AS gap_end,
               next_number - number - 1 AS gap_size
        FROM (
            SELECT number, LEAD(number) OVER (ORDER BY number) AS next_number
            FROM blocks
        ) sub
        WHERE next_number - number > 1
        ORDER BY gap_size DESC
        LIMIT 5
    """)
    if gaps:
        for gap_start, gap_end, gap_size in gaps:
            check("gap detected", WARN, f"blocks {gap_start:,}-{gap_end:,} ({gap_size:,} missing)")


def validate_duplicates(neon: NeonLoader) -> None:
    """Check for duplicate records."""
    print("\n3. DUPLICATE CHECKS")

    # Raw logs duplicates
    dup = neon.query("""
        SELECT COUNT(*) - COUNT(DISTINCT (transaction_hash, log_index)) AS duplicates
        FROM raw_logs
    """)
    if dup and dup[0][0] is not None:
        dup_count = dup[0][0]
        if dup_count == 0:
            check("raw_logs duplicates", PASS, "no duplicates")
        else:
            check("raw_logs duplicates", WARN, f"{dup_count:,} duplicate (tx_hash, log_index) pairs")

    # Decoded events duplicates
    dup = neon.query("""
        SELECT COUNT(*) - COUNT(DISTINCT (transaction_hash, log_index)) AS duplicates
        FROM decoded_events
    """)
    if dup and dup[0][0] is not None:
        dup_count = dup[0][0]
        if dup_count == 0:
            check("decoded_events duplicates", PASS, "no duplicates")
        else:
            check("decoded_events duplicates", WARN, f"{dup_count:,} duplicates")


def validate_foreign_keys(neon: NeonLoader) -> None:
    """Check FK integrity between tables."""
    print("\n4. FOREIGN KEY INTEGRITY")

    # Transactions → blocks
    orphan_txs = neon.query("""
        SELECT COUNT(*) FROM transactions t
        LEFT JOIN blocks b ON t.block_number = b.number
        WHERE b.number IS NULL
    """)
    if orphan_txs:
        count = orphan_txs[0][0]
        if count == 0:
            check("tx → blocks FK", PASS, "all transactions reference valid blocks")
        else:
            check("tx → blocks FK", WARN, f"{count:,} transactions reference missing blocks")

    # Decoded events → blocks
    orphan_events = neon.query("""
        SELECT COUNT(*) FROM decoded_events de
        LEFT JOIN blocks b ON de.block_number = b.number
        WHERE b.number IS NULL
    """)
    if orphan_events:
        count = orphan_events[0][0]
        if count == 0:
            check("events → blocks FK", PASS, "all events reference valid blocks")
        else:
            check("events → blocks FK", WARN, f"{count:,} events reference missing blocks")

    # Decoded events → transactions
    orphan_events_tx = neon.query("""
        SELECT COUNT(*) FROM decoded_events de
        LEFT JOIN transactions t ON de.transaction_hash = t.hash
        WHERE t.hash IS NULL
    """)
    if orphan_events_tx:
        count = orphan_events_tx[0][0]
        if count == 0:
            check("events → tx FK", PASS, "all events reference valid transactions")
        else:
            check("events → tx FK", WARN, f"{count:,} events reference missing transactions")


def validate_timestamps(neon: NeonLoader) -> None:
    """Check for suspicious timestamps."""
    print("\n5. TIMESTAMP VALIDATION")

    # Blocks with null timestamps
    null_ts = neon.query("SELECT COUNT(*) FROM blocks WHERE timestamp IS NULL")
    if null_ts:
        count = null_ts[0][0]
        total = neon.query("SELECT COUNT(*) FROM blocks")[0][0]
        if count == 0:
            check("block timestamps", PASS, "all blocks have timestamps")
        elif count < total * 0.01:
            check("block timestamps", WARN, f"{count:,}/{total:,} blocks missing timestamps")
        else:
            check("block timestamps", FAIL, f"{count:,}/{total:,} blocks missing timestamps ({count/total*100:.1f}%)")

    # Check timestamp ordering
    out_of_order = neon.query("""
        SELECT COUNT(*) FROM (
            SELECT number, timestamp,
                   LAG(timestamp) OVER (ORDER BY number) AS prev_ts
            FROM blocks WHERE timestamp IS NOT NULL
        ) sub
        WHERE timestamp < prev_ts
    """)
    if out_of_order:
        count = out_of_order[0][0]
        if count == 0:
            check("timestamp ordering", PASS, "blocks in chronological order")
        else:
            check("timestamp ordering", WARN, f"{count:,} blocks out of chronological order")


def validate_extraction_state(neon: NeonLoader) -> None:
    """Check extraction state table."""
    print("\n6. EXTRACTION STATE")
    states = neon.query("SELECT extraction_type, last_block_processed, total_items_processed, status FROM extraction_state ORDER BY extraction_type")
    if not states:
        check("extraction state", WARN, "no state records")
        return

    for ext_type, last_block, total_items, status in states:
        detail = f"block={last_block:,}, items={total_items:,}, status={status}"
        check(f"state: {ext_type}", PASS, detail)


def validate_rpc_sample(neon: NeonLoader, sample_size: int) -> None:
    """Sample random blocks from RPC and compare with DB."""
    print(f"\n7. RPC SAMPLE VALIDATION ({sample_size} blocks)")

    from dotenv import load_dotenv
    load_dotenv()
    from src.config import load_yaml
    from src.extractors.blockscout import BlockscoutExtractor

    chains = load_yaml("config/chains.yaml")
    cfg = chains["incentiv"]

    extractor = BlockscoutExtractor(
        base_url=cfg["blockscout_base_url"],
        rpc_url=cfg["blockscout_rpc_url"],
        confirmations=int(cfg["confirmations"]),
        batch_size=int(cfg["batch_size"]),
        rate_limit_per_second=float(cfg["rate_limit_per_second"]),
    )

    # Get block range from DB
    row = neon.query("SELECT MIN(number), MAX(number) FROM blocks")
    if not row or row[0][0] is None:
        check("RPC sample", WARN, "no blocks to sample")
        return

    min_b, max_b = row[0]
    sample_blocks = sorted(random.sample(range(min_b, max_b + 1), min(sample_size, max_b - min_b + 1)))

    mismatches = 0
    checked = 0

    for block_num in sample_blocks:
        try:
            # Get logs from RPC
            rpc_logs = extractor.get_all_logs(block_num, block_num)
            rpc_count = len(rpc_logs)

            # Get logs from DB
            db_result = neon.query(
                "SELECT COUNT(*) FROM raw_logs WHERE block_number = %s",
                (block_num,)
            )
            db_count = db_result[0][0] if db_result else 0

            if rpc_count != db_count:
                mismatches += 1
                if mismatches <= 5:
                    check(f"block {block_num}", WARN, f"RPC: {rpc_count} logs, DB: {db_count} logs")
            checked += 1

        except Exception as e:
            check(f"block {block_num}", WARN, f"RPC error: {e}")

    if mismatches == 0:
        check("RPC sample", PASS, f"{checked} blocks verified, all match")
    else:
        check("RPC sample", WARN, f"{mismatches}/{checked} blocks have mismatched log counts")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Validate Neon data quality")
    parser.add_argument("--quick", action="store_true", help="Quick check (counts only)")
    parser.add_argument("--rpc-sample", type=int, default=0, help="Number of blocks to sample from RPC")
    args = parser.parse_args()

    print("=" * 60)
    print("INCENTIV NEON DATA VALIDATION")
    print(f"Time: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    neon = NeonLoader()

    try:
        validate_counts(neon)

        if not args.quick:
            validate_block_continuity(neon)
            validate_duplicates(neon)
            validate_foreign_keys(neon)
            validate_timestamps(neon)
            validate_extraction_state(neon)

        if args.rpc_sample > 0:
            validate_rpc_sample(neon, args.rpc_sample)

        # Summary
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)
        pass_count = sum(1 for _, s, _ in results if s == PASS)
        warn_count = sum(1 for _, s, _ in results if s == WARN)
        fail_count = sum(1 for _, s, _ in results if s == FAIL)
        print(f"  PASS: {pass_count}  |  WARN: {warn_count}  |  FAIL: {fail_count}")

        if fail_count > 0:
            print("\n  FAILURES:")
            for name, status, detail in results:
                if status == FAIL:
                    print(f"    - {name}: {detail}")

    except Exception as e:
        print(f"\nValidation error: {e}")
        raise
    finally:
        neon.close()


if __name__ == "__main__":
    main()
