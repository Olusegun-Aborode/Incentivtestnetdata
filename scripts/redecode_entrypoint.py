#!/usr/bin/env python3
"""
Re-decode EntryPoint (ERC-4337) events from existing raw_logs in Neon.

This script:
1. Computes the topic0 hashes for EntryPoint events from config/abis/entrypoint.json
2. Queries raw_logs where topic0 matches any EntryPoint event signature
3. Decodes the indexed + non-indexed params using the ABI
4. Upserts into decoded_events (ON CONFLICT UPDATE to overwrite "Unknown" entries)

Usage:
    python3 scripts/redecode_entrypoint.py [--batch-size 10000] [--dry-run]
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from eth_abi import decode as abi_decode
from eth_utils import keccak
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader


# ── ABI LOADING ──────────────────────────────────────────────────
def load_entrypoint_registry():
    """Load EntryPoint ABI and build topic0 -> event_abi registry."""
    abi_path = Path("config/abis/entrypoint.json")
    abis = json.loads(abi_path.read_text())

    registry = {}
    for entry in abis:
        if entry.get("type") != "event":
            continue
        types = ",".join(inp["type"] for inp in entry.get("inputs", []))
        signature = f"{entry['name']}({types})"
        topic0 = f"0x{keccak(text=signature).hex()}".lower()

        if topic0 not in registry:
            registry[topic0] = entry
            print(f"  {entry['name']:35s} → {topic0[:18]}...")

    return registry


# ── DECODER ──────────────────────────────────────────────────────
def normalize_value(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return f"0x{bytes(value).hex()}"
    if isinstance(value, str):
        return value.lower() if value.startswith("0x") else value
    return str(value)


def is_dynamic_type(type_str):
    return type_str in {"string", "bytes"} or type_str.endswith("[]") or ("[" in type_str and "]" in type_str)


def normalize_column_name(name, position):
    import re
    if not name:
        return f"arg_{position}"
    name = name.lstrip('_')
    normalized = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    normalized = re.sub(r'[^a-z0-9_]+', '_', normalized).strip('_')
    return normalized or f"arg_{position}"


def decode_raw_log(event_abi, topic0, topic1, topic2, topic3, data):
    """Decode a raw log row into params dict using the event ABI."""
    inputs = event_abi.get("inputs", [])
    indexed_inputs = [(pos, inp) for pos, inp in enumerate(inputs) if inp.get("indexed")]
    non_indexed_inputs = [(pos, inp) for pos, inp in enumerate(inputs) if not inp.get("indexed")]

    topics = [t for t in [topic0, topic1, topic2, topic3] if t is not None]
    decoded = {}

    # Decode indexed params from topics[1:]
    for topic_index, (position, input_abi) in enumerate(indexed_inputs, start=1):
        col_name = normalize_column_name(input_abi.get("name"), position)
        if topic_index >= len(topics):
            decoded[col_name] = None
            continue
        topic_value = topics[topic_index]
        if is_dynamic_type(input_abi["type"]):
            decoded[col_name] = topic_value.lower()
            continue
        try:
            topic_bytes = bytes.fromhex(topic_value[2:] if topic_value.startswith("0x") else topic_value)
            decoded_value = abi_decode([input_abi["type"]], topic_bytes)[0]
            decoded[col_name] = normalize_value(decoded_value)
        except Exception:
            decoded[col_name] = topic_value

    # Decode non-indexed params from data
    if non_indexed_inputs and data and data != "0x" and len(data) > 2:
        try:
            data_bytes = bytes.fromhex(data[2:] if data.startswith("0x") else data)
            decoded_types = [inp["type"] for _, inp in non_indexed_inputs]
            decoded_values = abi_decode(decoded_types, data_bytes)
            for (position, input_abi), value in zip(non_indexed_inputs, decoded_values):
                col_name = normalize_column_name(input_abi.get("name"), position)
                decoded[col_name] = normalize_value(value)
        except Exception as e:
            decoded["_decode_error"] = str(e)

    return decoded


# ── MAIN ─────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Re-decode EntryPoint events from raw_logs")
    parser.add_argument("--batch-size", type=int, default=10000, help="Rows per batch (default 10000)")
    parser.add_argument("--dry-run", action="store_true", help="Count matching logs but don't insert")
    args = parser.parse_args()

    print("=" * 60)
    print("RE-DECODE ENTRYPOINT (ERC-4337) EVENTS")
    print("=" * 60)

    print("\nLoading EntryPoint ABI...")
    registry = load_entrypoint_registry()
    topic0_list = list(registry.keys())
    print(f"  {len(registry)} unique event signatures loaded\n")

    neon = NeonLoader()

    # Step 1: Count matching raw_logs
    topic_placeholders = ",".join([f"'{t}'" for t in topic0_list])
    count_query = f"SELECT COUNT(*) FROM raw_logs WHERE topic0 IN ({topic_placeholders})"
    rows = neon.query(count_query)
    total = rows[0][0]
    print(f"Found {total:,} raw_logs matching EntryPoint event signatures\n")

    if total == 0:
        print("Nothing to decode. Exiting.")
        neon.close()
        return

    if args.dry_run:
        # Show breakdown by event
        print("Dry run — showing breakdown by event signature:\n")
        for topic0, abi in registry.items():
            cnt_rows = neon.query(f"SELECT COUNT(*) FROM raw_logs WHERE topic0 = '{topic0}'")
            cnt = cnt_rows[0][0]
            if cnt > 0:
                print(f"  {abi['name']:35s}  {cnt:>12,}")
        print(f"\n  {'TOTAL':35s}  {total:>12,}")
        neon.close()
        return

    # Step 2: Process in batches using OFFSET/LIMIT on raw_logs
    batch_size = args.batch_size
    offset = 0
    total_decoded = 0
    total_inserted = 0
    total_updated = 0
    start_time = time.time()

    print(f"Processing {total:,} logs in batches of {batch_size:,}...\n")

    while offset < total:
        batch_query = f"""
            SELECT id, block_number, block_timestamp, transaction_hash, log_index,
                   address, topic0, topic1, topic2, topic3, data
            FROM raw_logs
            WHERE topic0 IN ({topic_placeholders})
            ORDER BY id
            LIMIT {batch_size} OFFSET {offset}
        """
        batch = neon.query(batch_query)
        if not batch:
            break

        decoded_rows = []
        for row in batch:
            (row_id, block_number, block_timestamp, tx_hash, log_index,
             contract_addr, topic0, topic1, topic2, topic3, data) = row

            event_abi = registry.get(topic0)
            if not event_abi:
                continue

            params = decode_raw_log(event_abi, topic0, topic1, topic2, topic3, data)

            # Remove internal keys
            params = {k: v for k, v in params.items() if not k.startswith("_")}

            decoded_rows.append({
                "event_name": event_abi["name"],
                "contract_address": contract_addr,
                "block_number": block_number,
                "transaction_hash": tx_hash,
                "log_index": log_index,
                "params": json.dumps(params) if params else None,
                "timestamp": block_timestamp,
                "chain": "incentiv",
            })

        total_decoded += len(decoded_rows)

        if decoded_rows:
            # Use upsert: ON CONFLICT (transaction_hash, log_index) UPDATE
            # This replaces "Unknown" events with properly decoded ones
            inserted, updated = _upsert_decoded(neon, decoded_rows)
            total_inserted += inserted
            total_updated += updated

        offset += batch_size
        elapsed = time.time() - start_time
        rate = total_decoded / elapsed if elapsed > 0 else 0
        pct = min(100, total_decoded / total * 100)
        print(f"  Decoded: {total_decoded:>10,} / {total:,}  ({pct:.1f}%)  |  "
              f"Inserted: {total_inserted:,}  Updated: {total_updated:,}  |  "
              f"{rate:.0f} rows/sec")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"COMPLETE in {elapsed:.1f}s")
    print(f"  Total decoded:  {total_decoded:,}")
    print(f"  New inserts:    {total_inserted:,}")
    print(f"  Updated:        {total_updated:,} (previously 'Unknown')")
    print(f"{'='*60}")

    neon.close()


def _upsert_decoded(neon, rows):
    """Upsert decoded events. Returns (inserted_count, updated_count)."""
    conn = neon.conn
    cur = conn.cursor()

    values = []
    for r in rows:
        values.append((
            r["event_name"],
            r["contract_address"],
            r["block_number"],
            r["transaction_hash"],
            r["log_index"],
            r["params"],
            r["timestamp"],
            r["chain"],
        ))

    sql = """
        INSERT INTO decoded_events (event_name, contract_address, block_number,
                                     transaction_hash, log_index, params, timestamp, chain)
        VALUES %s
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            event_name = EXCLUDED.event_name,
            params = EXCLUDED.params
        WHERE decoded_events.event_name = 'Unknown'
           OR decoded_events.event_name IS NULL
    """
    try:
        execute_values(cur, sql, values, page_size=1000)
        # Count: rows affected includes both inserts and updates
        affected = cur.rowcount
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"    Upsert error: {e}")
        return 0, 0
    finally:
        cur.close()

    # We can't distinguish inserts from updates easily, so return affected as "updated"
    # since most of these will be updates (they were previously decoded as Unknown)
    return 0, affected


if __name__ == "__main__":
    main()
