#!/usr/bin/env python3
"""
One-command backfill: December 2025 to present.

This script does everything:
1. Sets up the Neon schema
2. Finds the block number for December 1, 2025
3. Extracts ALL on-chain activity from that block to present
4. Loads everything into Neon
5. Refreshes analytics views

Usage:
    python scripts/backfill_from_december.py
    python scripts/backfill_from_december.py --start-date "2025-12-01"
    python scripts/backfill_from_december.py --batch-size 25
    python scripts/backfill_from_december.py --force-restart   # Ignore saved progress, re-process from start
    python scripts/backfill_from_december.py --force-restart --logs-only  # Re-process logs only (skip blocks/txs)
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.config import load_yaml
from src.extractors.blockscout import BlockscoutExtractor
from src.extractors.full_chain import FullChainExtractor
from src.loaders.neon import NeonLoader
from src.transformers.blocks import normalize_blocks
from src.transformers.transactions import normalize_transactions
from src.transformers.raw_logs import normalize_raw_logs
from src.transformers.decoded_logs import decode_logs
from src.handlers.dlq import DeadLetterQueue


def get_block_timestamp(extractor, block_num):
    blocks = extractor.get_blocks_by_number([block_num])
    if block_num not in blocks:
        return None
    return datetime.utcfromtimestamp(int(blocks[block_num]["timestamp"], 16))


def find_block_for_date(extractor, target_date):
    low = 0
    high = extractor.get_latest_block_number()
    best = low

    print(f"  Chain height: {high:,}")
    iterations = 0
    while low <= high and iterations < 50:
        mid = (low + high) // 2
        ts = get_block_timestamp(extractor, mid)
        iterations += 1
        if ts is None:
            mid += 1
            ts = get_block_timestamp(extractor, mid)
            if ts is None:
                high = mid - 2
                continue
        if ts < target_date:
            best = mid
            low = mid + 1
        else:
            high = mid - 1

    final_ts = get_block_timestamp(extractor, best)
    print(f"  Block {best:,} = {final_ts.strftime('%Y-%m-%d %H:%M UTC')}")
    return best


def main():
    parser = argparse.ArgumentParser(description="Backfill Incentiv from December to present")
    parser.add_argument("--start-date", default="2025-12-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=50, help="Blocks per batch (default 50)")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    parser.add_argument("--force-restart", action="store_true",
                        help="Ignore saved progress and re-process from start date. "
                             "Blocks/txs already in Neon won't be duplicated (ON CONFLICT DO NOTHING).")
    parser.add_argument("--logs-only", action="store_true",
                        help="Only extract and insert raw_logs, decoded_events, and contracts. "
                             "Skip blocks/txs insertion (use when blocks/txs already exist in Neon).")
    args = parser.parse_args()

    target_date = datetime.strptime(args.start_date, "%Y-%m-%d")

    print("=" * 60)
    mode = "LOGS-ONLY BACKFILL" if args.logs_only else "FULL BACKFILL"
    print(f"INCENTIV {mode}")
    print(f"From: {args.start_date}")
    print(f"To:   now")
    print("=" * 60)

    # Connect to blockchain
    chains = load_yaml("config/chains.yaml")
    cfg = chains["incentiv"]

    extractor = BlockscoutExtractor(
        base_url=cfg["blockscout_base_url"],
        rpc_url=cfg["blockscout_rpc_url"],
        confirmations=int(cfg["confirmations"]),
        batch_size=args.batch_size,
        rate_limit_per_second=float(cfg["rate_limit_per_second"]),
    )

    # Step 1: Setup Neon schema
    print("\n[1/4] Setting up Neon schema...")
    neon = NeonLoader()
    try:
        neon.setup_schema()
        print("  Schema ready.")
    except Exception as e:
        print(f"  Schema setup note: {e}")
        print("  (Tables may already exist — continuing)")

    # Step 2: Find start block
    print(f"\n[2/4] Finding block for {args.start_date}...")
    start_block = find_block_for_date(extractor, target_date)

    safe_block = extractor.get_safe_block_number()
    total_blocks = safe_block - start_block
    print(f"  Will process {total_blocks:,} blocks ({start_block:,} to {safe_block:,})")

    # Check for resume (unless --force-restart)
    if args.force_restart:
        print(f"\n  --force-restart: Ignoring saved progress. Starting from block {start_block:,}")
        print(f"  (Blocks/txs already in Neon will be skipped via ON CONFLICT DO NOTHING)")
        # Reset the extraction state so it doesn't confuse future runs
        neon.update_extraction_state("all_activity", start_block, total_items=0, status="restarting")
    else:
        state = neon.get_extraction_state("all_activity")
        if state["last_block_processed"] > start_block:
            resume_block = state["last_block_processed"] + 1
            remaining = safe_block - resume_block
            print(f"\n  Resuming from block {resume_block:,} ({remaining:,} blocks remaining)")
            start_block = resume_block

    if args.dry_run:
        est_hours = total_blocks / (args.batch_size * 3) / 3600  # rough estimate
        print(f"\n  [DRY RUN] Estimated time: {est_hours:.1f} hours")
        print(f"  Run without --dry-run to start.")
        neon.close()
        return

    # Step 3: Extract and load
    print(f"\n[3/4] Extracting all on-chain activity...")
    full_extractor = FullChainExtractor(extractor)
    dlq = DeadLetterQueue()

    total_blocks_done = 0
    total_txs = 0
    total_logs = 0
    total_logs_inserted = 0
    total_decoded = 0
    total_contracts = 0
    start_time = time.time()
    batch_size = args.batch_size

    try:
        for batch_start in range(start_block, safe_block + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, safe_block)

            try:
                result = full_extractor.extract_full_batch(batch_start, batch_end)
                blocks = result["blocks"]
                txs = result["transactions"]
                logs = result["logs"]

                # Load blocks and transactions (skip if --logs-only)
                if not args.logs_only:
                    if blocks:
                        df_blocks = normalize_blocks(blocks, chain="incentiv")
                        neon.copy_dataframe("blocks", df_blocks)
                    if txs:
                        df_txs = normalize_transactions(blocks, chain="incentiv")
                        neon.copy_dataframe("transactions", df_txs)

                # Load raw logs (independent try/catch so failure doesn't skip decoding)
                if logs:
                    try:
                        df_raw = normalize_raw_logs(logs, chain="incentiv")
                        inserted = neon.copy_dataframe("raw_logs", df_raw)
                        total_logs_inserted += inserted
                        if inserted == 0 and len(df_raw) > 0:
                            print(f"    WARNING: 0 raw_logs inserted from {len(df_raw)} rows")
                    except Exception as log_err:
                        print(f"    raw_logs insert error: {log_err}")

                    # Decode known events (independent from raw_logs)
                    try:
                        decoded_tables = decode_logs(
                            logs=logs, chain="incentiv",
                            abi_dir=Path("config/abis"),
                            include_unknown=True,
                        )
                        for table_key, decoded_df in decoded_tables.items():
                            if not decoded_df.empty:
                                _load_decoded(neon, decoded_df)
                                total_decoded += len(decoded_df)
                    except Exception as dec_err:
                        print(f"    decoded_events insert error: {dec_err}")

                    # Track contracts (independent)
                    try:
                        contracts = full_extractor.discover_contracts(logs)
                        if contracts:
                            neon.upsert_contracts(list(contracts.values()))
                            total_contracts += len(contracts)
                    except Exception as con_err:
                        print(f"    contracts upsert error: {con_err}")

                total_blocks_done += len(blocks)
                total_txs += len(txs)
                total_logs += len(logs)

                # Update state
                neon.update_extraction_state(
                    "all_activity", batch_end,
                    total_items=len(blocks) + len(txs) + len(logs),
                    status="running"
                )

                # Progress report
                elapsed = time.time() - start_time
                progress = (batch_end - start_block) / max(safe_block - start_block, 1) * 100
                rate = total_blocks_done / elapsed if elapsed > 0 else 0
                eta = (safe_block - batch_end) / rate / 3600 if rate > 0 else 0

                print(f"  [{progress:5.1f}%] block {batch_end:,} | "
                      f"{total_blocks_done:,} blks, {total_txs:,} txs, "
                      f"{total_logs_inserted:,} logs, {total_decoded:,} decoded, "
                      f"{total_contracts:,} contracts | {rate:.1f} b/s | ETA: {eta:.1f}h")

            except Exception as e:
                print(f"\n  Error at batch {batch_start}-{batch_end}: {e}")
                dlq.send(
                    record={"batch": f"{batch_start}-{batch_end}"},
                    error=e,
                    context={"from_block": batch_start, "to_block": batch_end},
                )
                print("  Saved to DLQ. Continuing with next batch...")
                continue

    except KeyboardInterrupt:
        print(f"\n\nStopped by user. Progress saved at block {batch_end:,}.")
        print("Run this script again to resume from where you left off.")

    # Step 4: Refresh views
    print(f"\n[4/4] Refreshing analytics views...")
    try:
        neon.refresh_materialized_views()
        print("  Views refreshed.")
    except Exception as e:
        print(f"  Views refresh note: {e}")

    # Summary
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"BACKFILL COMPLETE")
    print(f"{'='*60}")
    print(f"  Blocks:         {total_blocks_done:,}")
    print(f"  Transactions:   {total_txs:,}")
    print(f"  Raw logs:       {total_logs_inserted:,}")
    print(f"  Decoded events: {total_decoded:,}")
    print(f"  Contracts:      {total_contracts:,}")
    print(f"  Duration:       {elapsed/3600:.1f} hours")
    print(f"\nNext steps:")
    print(f"  python scripts/validate_neon.py             # Check data quality")
    print(f"  python scripts/generate_charts.py --interactive  # Generate charts")

    neon.close()


def _load_decoded(neon, decoded_df):
    """Convert decoded DataFrame to decoded_events format and load."""
    import json
    import pandas as pd

    base_cols = ["block_number", "block_timestamp", "tx_hash", "log_index",
                 "address", "event_name", "chain", "extracted_at"]
    rows = []
    for _, row in decoded_df.iterrows():
        params = {}
        for col in decoded_df.columns:
            if col not in base_cols:
                val = row.get(col)
                if pd.notna(val) and val is not None and val != "":
                    params[col] = str(val)

        rows.append({
            "event_name": row.get("event_name", "Unknown"),
            "contract_address": row.get("address", ""),
            "block_number": int(row.get("block_number", 0)),
            "transaction_hash": row.get("tx_hash", ""),
            "log_index": int(row.get("log_index", 0)),
            "params": json.dumps(params) if params else None,
            "timestamp": row.get("block_timestamp"),
            "chain": row.get("chain", "incentiv"),
        })

    if rows:
        df = pd.DataFrame(rows)
        neon.copy_dataframe("decoded_events", df)


if __name__ == "__main__":
    main()
