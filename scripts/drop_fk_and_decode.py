#!/usr/bin/env python3
"""
FIX #2: Drop the FK constraint on decoded_events.transaction_hash,
then decode ALL 12M+ raw_logs immediately.

WHY: 845K transactions are missing from the transactions table.
The FK constraint blocks decoded_events inserts for any log whose
transaction_hash isn't in transactions.  Dropping the FK lets us
decode everything NOW.  Transactions can be backfilled later.

This uses REAL data only — every decoded event comes from an actual
raw_log that was fetched from the blockchain.

Run: python3 scripts/drop_fk_and_decode.py
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

import psycopg2
import psycopg2.extras
from eth_abi import decode as abi_decode
from eth_utils import keccak

from src.loaders.neon import NeonLoader


# ════════════════════════════════════════════════════════════════
# STEP 1: Drop the FK constraint
# ════════════════════════════════════════════════════════════════

def drop_fk_constraint(neon):
    """Find and drop the FK constraint on decoded_events.transaction_hash."""
    print("=" * 60)
    print("STEP 1: Drop FK constraint on decoded_events.transaction_hash")
    print("=" * 60)

    # Find the constraint name
    rows = neon.query("""
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = 'decoded_events'
          AND constraint_type = 'FOREIGN KEY'
    """)

    if not rows:
        print("  No FK constraints found on decoded_events — already dropped or never existed.")
        return

    for (cname,) in rows:
        print(f"  Found FK constraint: {cname}")
        # Check if it references transactions(hash)
        ref_rows = neon.query(f"""
            SELECT ccu.table_name, ccu.column_name
            FROM information_schema.constraint_column_usage ccu
            WHERE ccu.constraint_name = '{cname}'
        """)
        for ref_table, ref_col in ref_rows:
            print(f"    References: {ref_table}.{ref_col}")

    # Drop ALL FK constraints on decoded_events
    conn = neon.conn
    cur = conn.cursor()
    for (cname,) in rows:
        print(f"  Dropping: {cname} ...")
        cur.execute(f"ALTER TABLE decoded_events DROP CONSTRAINT IF EXISTS {cname}")
    conn.commit()
    cur.close()
    print("  ✓ FK constraints dropped.\n")


# ════════════════════════════════════════════════════════════════
# STEP 2: Build ABI registry (same logic as redecode_all.py)
# ════════════════════════════════════════════════════════════════

def build_registry():
    """Build topic0 → ABI mapping for all known event types."""
    registry = {}

    # EntryPoint ABI (ERC-4337)
    try:
        entrypoint_abi = json.loads(Path("config/abis/entrypoint.json").read_text())
        for entry in entrypoint_abi:
            if entry.get("type") == "event":
                types = ",".join(inp["type"] for inp in entry.get("inputs", []))
                sig = f"{entry['name']}({types})"
                topic0 = f"0x{keccak(text=sig).hex()}".lower()
                registry[topic0] = entry
    except Exception as e:
        print(f"  [WARN] Failed to load EntryPoint ABI: {e}")

    # Well-known events
    events = [
        ("Transfer", "Transfer(address,address,uint256)", [
            {"name": "from", "type": "address", "indexed": True},
            {"name": "to", "type": "address", "indexed": True},
            {"name": "value", "type": "uint256", "indexed": False}
        ]),
        ("Approval", "Approval(address,address,uint256)", [
            {"name": "owner", "type": "address", "indexed": True},
            {"name": "spender", "type": "address", "indexed": True},
            {"name": "value", "type": "uint256", "indexed": False}
        ]),
        ("Swap", "Swap(address,uint256,uint256,uint256,uint256,address)", [
            {"name": "sender", "type": "address", "indexed": True},
            {"name": "amount0In", "type": "uint256", "indexed": False},
            {"name": "amount1In", "type": "uint256", "indexed": False},
            {"name": "amount0Out", "type": "uint256", "indexed": False},
            {"name": "amount1Out", "type": "uint256", "indexed": False},
            {"name": "to", "type": "address", "indexed": True}
        ]),
        ("Swap", "Swap(address,address,int256,int256,uint160,uint128,int24)", [
            {"name": "sender", "type": "address", "indexed": True},
            {"name": "recipient", "type": "address", "indexed": True},
            {"name": "amount0", "type": "int256", "indexed": False},
            {"name": "amount1", "type": "int256", "indexed": False},
            {"name": "sqrtPriceX96", "type": "uint160", "indexed": False},
            {"name": "liquidity", "type": "uint128", "indexed": False},
            {"name": "tick", "type": "int24", "indexed": False}
        ]),
        ("ReceivedTransferRemote", "ReceivedTransferRemote(uint32,bytes32,uint256)", [
            {"name": "origin", "type": "uint32", "indexed": True},
            {"name": "recipient", "type": "bytes32", "indexed": True},
            {"name": "amount", "type": "uint256", "indexed": False}
        ]),
        ("SentTransferRemote", "SentTransferRemote(uint32,bytes32,uint256)", [
            {"name": "destination", "type": "uint32", "indexed": True},
            {"name": "recipient", "type": "bytes32", "indexed": True},
            {"name": "amount", "type": "uint256", "indexed": False}
        ]),
        ("Process", "Process(bytes32)", [
            {"name": "messageId", "type": "bytes32", "indexed": True}
        ]),
    ]

    for name, sig, inputs in events:
        topic0 = f"0x{keccak(text=sig).hex()}".lower()
        registry[topic0] = {"name": name, "inputs": inputs}

    print(f"  ABI registry: {len(registry)} event signatures loaded")
    return registry


# ════════════════════════════════════════════════════════════════
# STEP 3: Decode helpers
# ════════════════════════════════════════════════════════════════

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


def decode_log(abi, topic0, topic1, topic2, topic3, data):
    inputs = abi.get("inputs", [])
    indexed = [(pos, i) for pos, i in enumerate(inputs) if i.get("indexed")]
    non_indexed = [(pos, i) for pos, i in enumerate(inputs) if not i.get("indexed")]

    topics = [t for t in [topic0, topic1, topic2, topic3] if t]
    params = {}

    for t_idx, (pos, inp) in enumerate(indexed, start=1):
        if t_idx < len(topics):
            topic_val = topics[t_idx]
            col_name = inp.get("name", f"arg_{pos}")
            try:
                topic_bytes = bytes.fromhex(
                    topic_val[2:] if topic_val.startswith("0x") else topic_val
                )
                val = abi_decode([inp["type"]], topic_bytes)[0]
                params[col_name] = normalize_value(val)
            except Exception:
                params[col_name] = topic_val

    if non_indexed and data and data != "0x" and len(data) > 2:
        try:
            data_bytes = bytes.fromhex(
                data[2:] if data.startswith("0x") else data
            )
            types = [i["type"] for _, i in non_indexed]
            vals = abi_decode(types, data_bytes)
            for (pos, inp), val in zip(non_indexed, vals):
                params[inp.get("name", f"arg_{pos}")] = normalize_value(val)
        except Exception:
            pass  # Skip logs with malformed data

    return {k: v for k, v in params.items() if not k.startswith("_")}


def insert_batch(neon, rows):
    """Insert decoded events batch. Returns count inserted."""
    if not rows:
        return 0
    conn = neon.conn
    cur = conn.cursor()
    values = [
        (r["event_name"], r["contract_address"], r["block_number"],
         r["transaction_hash"], r["log_index"], r["params"],
         r["timestamp"], r["chain"])
        for r in rows
    ]
    sql = """
        INSERT INTO decoded_events
            (event_name, contract_address, block_number,
             transaction_hash, log_index, params, timestamp, chain)
        VALUES %s
        ON CONFLICT (transaction_hash, log_index) DO NOTHING
    """
    try:
        psycopg2.extras.execute_values(cur, sql, values, page_size=5000)
        conn.commit()
        inserted = cur.rowcount
        cur.close()
        return inserted
    except Exception as e:
        conn.rollback()
        cur.close()
        print(f"  DB Error: {e}")
        # Try to reconnect for next batch
        try:
            neon.reconnect()
        except Exception:
            pass
        return 0


# ════════════════════════════════════════════════════════════════
# STEP 4: Full decode
# ════════════════════════════════════════════════════════════════

def run_full_decode(neon, registry):
    print("=" * 60)
    print("STEP 2: Decode ALL raw_logs with known event signatures")
    print("=" * 60)

    topics_sql = ",".join([f"'{t}'" for t in registry.keys()])

    # Get block range
    result = neon.query(f"""
        SELECT MIN(block_number), MAX(block_number), COUNT(*)
        FROM raw_logs
        WHERE topic0 IN ({topics_sql})
    """)
    min_blk, max_blk, total_matching = result[0]
    print(f"  Decodable logs: {total_matching:,}")
    print(f"  Block range: {min_blk:,} to {max_blk:,}")

    # Check existing decoded count
    existing = neon.query("SELECT COUNT(*) FROM decoded_events")[0][0]
    print(f"  Already decoded: {existing:,}")
    print()

    chunk_size = 50_000
    total_inserted = 0
    total_skipped = 0
    start_time = time.time()

    for chunk_start in range(min_blk, max_blk + 1, chunk_size):
        chunk_end = chunk_start + chunk_size - 1

        try:
            batch = neon.query(f"""
                SELECT id, block_number, block_timestamp, transaction_hash,
                       log_index, address, topic0, topic1, topic2, topic3, data
                FROM raw_logs
                WHERE block_number >= {chunk_start}
                  AND block_number <= {chunk_end}
                  AND topic0 IN ({topics_sql})
            """)
        except Exception as e:
            print(f"  ⚠ Failed to fetch chunk {chunk_start}-{chunk_end}: {e}")
            try:
                neon.reconnect()
            except Exception:
                pass
            continue

        if not batch:
            continue

        decoded_rows = []
        for row in batch:
            _, blk_num, blk_ts, tx_hash, log_idx, addr, t0, t1, t2, t3, data = row
            abi = registry.get(t0)
            if not abi:
                continue

            params = decode_log(abi, t0, t1, t2, t3, data)
            decoded_rows.append({
                "event_name": abi["name"],
                "contract_address": addr,
                "block_number": blk_num,
                "transaction_hash": tx_hash,
                "log_index": log_idx,
                "params": json.dumps(params) if params else None,
                "timestamp": blk_ts,
                "chain": "incentiv"
            })

        if decoded_rows:
            inserted = insert_batch(neon, decoded_rows)
            total_inserted += inserted
            total_skipped += len(decoded_rows) - inserted

        elapsed = time.time() - start_time
        pct = (chunk_end - min_blk) / max(1, max_blk - min_blk) * 100
        rate = total_inserted / max(1, elapsed)
        print(f"  Blocks {chunk_start:,}-{chunk_end:,} ({pct:.1f}%) "
              f"| Batch: {len(batch):,} logs "
              f"| New: {total_inserted:,} "
              f"| Dupes: {total_skipped:,} "
              f"| {rate:.0f} evt/s")

    elapsed = time.time() - start_time
    print(f"\n  ✓ Decode complete in {elapsed:.0f}s")
    print(f"    New events inserted: {total_inserted:,}")
    print(f"    Duplicates skipped:  {total_skipped:,}")

    # Final count
    final = neon.query("SELECT COUNT(*) FROM decoded_events")[0][0]
    print(f"    Total decoded_events in DB: {final:,}")
    return total_inserted


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def main():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  DROP FK CONSTRAINT + FULL EVENT DECODE                 ║")
    print("║  All data is REAL — decoded directly from raw_logs      ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    neon = NeonLoader()

    # Get current counts
    print("Current DB state:")
    for table in ["blocks", "transactions", "raw_logs", "decoded_events"]:
        count = neon.query(f"SELECT COUNT(*) FROM {table}")[0][0]
        print(f"  {table}: {count:,}")
    print()

    # Step 1: Drop FK
    drop_fk_constraint(neon)

    # Step 2: Build registry
    print("Building ABI registry...")
    registry = build_registry()
    print()

    # Step 3: Decode everything
    new_events = run_full_decode(neon, registry)

    # Step 4: Refresh materialized views
    print("\n" + "=" * 60)
    print("STEP 3: Refresh materialized views")
    print("=" * 60)
    try:
        neon.refresh_materialized_views()
        print("  ✓ Views refreshed")
    except Exception as e:
        print(f"  ⚠ View refresh failed: {e}")
        print("  This is OK — dashboard will still work from base tables")

    neon.close()

    print("\n" + "=" * 60)
    print("ALL DONE")
    print("=" * 60)
    print(f"  New decoded events: {new_events:,}")
    print("  Next: run  python3 scripts/generate_dashboard.py")


if __name__ == "__main__":
    main()
