#!/usr/bin/env python3
"""
Enhanced backup CSV importer for Neon PostgreSQL.

Imports ALL backup CSVs (blocks, transactions, raw logs, decoded events)
into Neon with proper ordering (FK dependencies), progress tracking,
and duplicate-safe upserts.

Usage:
    python scripts/import_backups_to_neon.py --all              # Import everything
    python scripts/import_backups_to_neon.py --blocks           # Just blocks
    python scripts/import_backups_to_neon.py --all --setup-schema  # Setup schema first
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader


# ---------------------------------------------------------------------------
# CSV → DataFrame normalization per table type
# ---------------------------------------------------------------------------

def normalize_block_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Map block CSV columns to Neon schema."""
    # CSV uses block_number, schema PK is number
    if "block_number" in df.columns and "number" not in df.columns:
        df = df.rename(columns={"block_number": "number"})

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    if "extracted_at" in df.columns:
        df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce", utc=True)

    for col in ["gas_used", "gas_limit", "size", "transaction_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    schema_cols = [
        "number", "hash", "parent_hash", "timestamp", "gas_used", "gas_limit",
        "base_fee_per_gas", "miner", "difficulty", "total_difficulty", "size",
        "extra_data", "nonce", "sha3_uncles", "logs_bloom", "transactions_root",
        "state_root", "receipts_root", "transaction_count", "chain", "extracted_at"
    ]
    available = [c for c in schema_cols if c in df.columns]
    return df[available]


def normalize_transaction_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Map transaction CSV columns to Neon schema."""
    for col in ["block_timestamp", "timestamp", "extracted_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    if "timestamp" not in df.columns and "block_timestamp" in df.columns:
        df["timestamp"] = df["block_timestamp"]

    schema_cols = [
        "hash", "block_number", "from_address", "to_address", "value",
        "gas_price", "gas", "gas_used", "input", "input_data", "status",
        "nonce", "transaction_index", "block_hash", "block_timestamp",
        "timestamp", "chain", "extracted_at"
    ]
    available = [c for c in schema_cols if c in df.columns]
    return df[available]


def normalize_raw_log_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw log CSV columns to raw_logs Neon schema."""
    col_map = {"tx_hash": "transaction_hash"}
    df = df.rename(columns=col_map)

    if "block_timestamp" in df.columns:
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], errors="coerce", utc=True)
    if "extracted_at" in df.columns:
        df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce", utc=True)

    schema_cols = [
        "block_number", "transaction_hash", "log_index", "address",
        "topic0", "topic1", "topic2", "topic3", "data",
        "block_timestamp", "chain", "extracted_at"
    ]
    available = [c for c in schema_cols if c in df.columns]
    return df[available]


def normalize_decoded_log_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Map decoded log CSV to decoded_events schema with JSONB params."""
    col_map = {
        "tx_hash": "transaction_hash",
        "address": "contract_address",
        "block_timestamp": "timestamp",
    }
    df = df.rename(columns=col_map)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    base_cols = [
        "event_name", "contract_address", "block_number",
        "transaction_hash", "log_index", "timestamp", "chain"
    ]

    # Everything else becomes params JSONB
    extra_cols = [c for c in df.columns if c not in base_cols and c not in ["extracted_at"]]

    def build_params(row):
        params = {}
        for col in extra_cols:
            val = row.get(col)
            if pd.notna(val) and val != "" and val is not None:
                params[col] = str(val) if not isinstance(val, str) else val
        return json.dumps(params) if params else None

    df["params"] = df.apply(build_params, axis=1)
    result_cols = [c for c in base_cols if c in df.columns] + ["params"]
    return df[result_cols]


# ---------------------------------------------------------------------------
# Import orchestration
# ---------------------------------------------------------------------------

def import_csv_files(
    loader: NeonLoader,
    csv_dir: str,
    pattern: str,
    table: str,
    normalizer,
    state_key: str,
    batch_commit: int = 500,
) -> int:
    """Import CSV files into Neon with progress tracking and error handling."""
    csv_files = sorted(Path(csv_dir).glob(pattern))
    if not csv_files:
        print(f"  No files matching {pattern} in {csv_dir}")
        return 0

    # Check resume state
    state = loader.get_extraction_state(state_key)
    files_done = state.get("total_items_processed", 0)

    total_files = len(csv_files)
    if files_done > 0 and files_done < total_files:
        print(f"  Resuming from file #{files_done + 1} (previously imported {files_done:,})")
        csv_files = csv_files[files_done:]
    elif files_done >= total_files:
        print(f"  Already completed ({files_done:,} files imported)")
        return 0

    total = total_files
    total_rows = 0
    errors = 0
    start_time = time.time()

    loader.update_extraction_state(state_key, 0, files_done, status="running")

    for i, csv_file in enumerate(csv_files, start=files_done + 1):
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            if df.empty:
                continue

            df_normalized = normalizer(df)
            rows = loader.copy_dataframe(table, df_normalized)
            total_rows += rows

            if i % batch_commit == 0:
                elapsed = time.time() - start_time
                rate = (i - files_done) / elapsed if elapsed > 0 else 0
                remaining = total - i
                eta = remaining / rate if rate > 0 else 0
                print(f"  [{i:,}/{total:,}] +{rows} rows | Total: {total_rows:,} | "
                      f"{rate:.1f} files/sec | ETA: {eta:.0f}s")
                loader.update_extraction_state(state_key, 0, i, status="running")

        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  Error on {csv_file.name}: {e}")
            elif errors == 11:
                print("  (suppressing further error messages...)")
            continue

    elapsed = time.time() - start_time
    loader.update_extraction_state(state_key, 0, total, status="completed")
    print(f"  Done: {total_rows:,} rows from {total:,} files "
          f"({errors} errors) in {elapsed:.1f}s")
    return total_rows


def main():
    parser = argparse.ArgumentParser(description="Import backup CSVs to Neon")
    parser.add_argument("--blocks", action="store_true", help="Import block CSVs")
    parser.add_argument("--transactions", action="store_true", help="Import transaction CSVs")
    parser.add_argument("--logs", action="store_true", help="Import raw log CSVs")
    parser.add_argument("--decoded", action="store_true", help="Import decoded event CSVs")
    parser.add_argument("--all", action="store_true", help="Import everything")
    parser.add_argument("--setup-schema", action="store_true", help="Run schema setup first")
    args = parser.parse_args()

    if not any([args.blocks, args.transactions, args.logs, args.decoded, args.all]):
        args.all = True

    loader = NeonLoader()

    if args.setup_schema:
        print("Setting up Neon schema...")
        loader.setup_schema()

    print("=" * 60)
    print("INCENTIV BACKUP IMPORT TO NEON")
    print(f"Started: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    try:
        # Phase 1: Blocks (FK parent — must be first)
        if args.all or args.blocks:
            print(f"\n1. IMPORTING BLOCKS (backups/blocks/)...")
            import_csv_files(
                loader=loader,
                csv_dir="backups/blocks",
                pattern="*.csv",
                table="blocks",
                normalizer=normalize_block_csv,
                state_key="csv_import_blocks",
                batch_commit=1000,
            )

        # Phase 2: Transactions (FK depends on blocks)
        if args.all or args.transactions:
            print(f"\n2. IMPORTING TRANSACTIONS (backups/transactions/)...")
            import_csv_files(
                loader=loader,
                csv_dir="backups/transactions",
                pattern="*.csv",
                table="transactions",
                normalizer=normalize_transaction_csv,
                state_key="csv_import_transactions",
                batch_commit=1000,
            )

        # Phase 3: Raw logs
        if args.all or args.logs:
            print(f"\n3. IMPORTING RAW LOGS (backups/logs/)...")
            import_csv_files(
                loader=loader,
                csv_dir="backups/logs",
                pattern="*.csv",
                table="raw_logs",
                normalizer=normalize_raw_log_csv,
                state_key="csv_import_logs",
                batch_commit=500,
            )

        # Phase 4: Decoded events (FK depends on blocks + transactions)
        if args.all or args.decoded:
            print(f"\n4. IMPORTING DECODED EVENTS (backups/decoded_logs/)...")
            import_csv_files(
                loader=loader,
                csv_dir="backups/decoded_logs",
                pattern="*.csv",
                table="decoded_events",
                normalizer=normalize_decoded_log_csv,
                state_key="csv_import_decoded",
                batch_commit=500,
            )

        # Final summary
        print("\n" + "=" * 60)
        print("IMPORT SUMMARY")
        print("=" * 60)
        counts = loader.get_table_counts()
        for table, count in counts.items():
            status = f"{count:,}" if count >= 0 else "N/A"
            print(f"  {table}: {status} rows")

    except KeyboardInterrupt:
        print("\nInterrupted. Progress saved — run again to resume.")
    except Exception as e:
        print(f"\nFatal error: {e}")
        raise
    finally:
        loader.close()


if __name__ == "__main__":
    main()
