#!/usr/bin/env python3
"""
Diagnose the REAL unique user counts by joining decoded_events with transactions.
Also identifies the remaining Unknown events by topic0.

Usage: python3 scripts/diagnose_users.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')
from src.loaders.neon import NeonLoader


def main():
    neon = NeonLoader()

    print("=" * 70)
    print("REAL USER DIAGNOSIS — JOIN decoded_events WITH transactions")
    print("=" * 70)

    # ── SWAPS: Real users via tx from_address ──
    print("\n[SWAPS]")
    print("  Event param 'sender' (router contracts):")
    rows = neon.query("""
        SELECT COUNT(DISTINCT params->>'sender') FROM decoded_events
        WHERE event_name = 'Swap' AND params IS NOT NULL
    """)
    print(f"    Unique swap senders (from params): {rows[0][0]:,}")

    print("  Real users via transactions.from_address:")
    rows = neon.query("""
        SELECT COUNT(DISTINCT t.from_address)
        FROM decoded_events de
        JOIN transactions t ON de.transaction_hash = t.hash
        WHERE de.event_name = 'Swap'
    """)
    print(f"    Unique swap users (from tx): {rows[0][0]:,}")

    print("  Top swap users (by from_address):")
    rows = neon.query("""
        SELECT t.from_address, COUNT(*) as cnt
        FROM decoded_events de
        JOIN transactions t ON de.transaction_hash = t.hash
        WHERE de.event_name = 'Swap'
        GROUP BY t.from_address ORDER BY cnt DESC LIMIT 10
    """)
    for r in rows:
        print(f"    {r[0][:12]}...{r[0][-6:]}  {r[1]:>10,} swaps")

    # ── TRANSFERS: Real users via tx from_address ──
    print("\n[TRANSFERS]")
    print("  Event param 'from' (token-level senders):")
    rows = neon.query("""
        SELECT COUNT(DISTINCT params->>'from') FROM decoded_events
        WHERE event_name = 'Transfer' AND params IS NOT NULL
    """)
    print(f"    Unique transfer senders (from params): {rows[0][0]:,}")

    print("  Real users via transactions.from_address:")
    rows = neon.query("""
        SELECT COUNT(DISTINCT t.from_address)
        FROM decoded_events de
        JOIN transactions t ON de.transaction_hash = t.hash
        WHERE de.event_name = 'Transfer'
    """)
    print(f"    Unique transfer initiators (from tx): {rows[0][0]:,}")

    # ── BRIDGE: Real users via tx from_address ──
    print("\n[BRIDGE]")
    print("  Event param 'recipient' (bridge recipients):")
    rows = neon.query("""
        SELECT COUNT(DISTINCT params->>'recipient') FROM decoded_events
        WHERE event_name IN ('SentTransferRemote','ReceivedTransferRemote')
        AND params->>'recipient' IS NOT NULL
    """)
    print(f"    Unique bridge recipients (from params): {rows[0][0]:,}")

    print("  Real bridge users via transactions.from_address:")
    rows = neon.query("""
        SELECT COUNT(DISTINCT t.from_address)
        FROM decoded_events de
        JOIN transactions t ON de.transaction_hash = t.hash
        WHERE de.event_name IN ('SentTransferRemote','ReceivedTransferRemote','Dispatch','Process')
    """)
    print(f"    Unique bridge users (from tx): {rows[0][0]:,}")

    # ── OVERVIEW: Total unique users across ALL activity ──
    print("\n[OVERVIEW — TOTAL UNIQUE USERS]")
    rows = neon.query("SELECT COUNT(DISTINCT from_address) FROM transactions")
    print(f"  Total unique tx senders (from_address): {rows[0][0]:,}")
    rows = neon.query("SELECT COUNT(DISTINCT to_address) FROM transactions WHERE to_address IS NOT NULL")
    print(f"  Total unique tx receivers (to_address): {rows[0][0]:,}")

    # ── UNKNOWN EVENTS BREAKDOWN ──
    print("\n" + "=" * 70)
    print("REMAINING UNKNOWN EVENTS — by topic0")
    print("=" * 70)
    rows = neon.query("""
        SELECT rl.topic0, rl.address, COUNT(*) as cnt
        FROM raw_logs rl
        LEFT JOIN decoded_events de ON rl.transaction_hash = de.transaction_hash
                                    AND rl.log_index = de.log_index
        WHERE de.event_name = 'Unknown' OR de.id IS NULL
        GROUP BY rl.topic0, rl.address
        ORDER BY cnt DESC
        LIMIT 20
    """)
    print(f"\n  Top 20 Unknown event signatures:\n")
    print(f"  {'topic0':20s} {'contract':20s} {'count':>12s}")
    print(f"  {'-'*20} {'-'*20} {'-'*12}")
    for r in rows:
        topic = r[0][:18] + '...' if r[0] else 'NULL'
        addr = r[1][:12] + '...' + r[1][-6:] if r[1] else 'NULL'
        print(f"  {topic:20s} {addr:20s} {r[2]:>12,}")

    # ── Check what decoded_events has as Unknown by contract ──
    print("\n\nUNKNOWN events in decoded_events by contract:")
    rows = neon.query("""
        SELECT contract_address, COUNT(*) as cnt
        FROM decoded_events
        WHERE event_name = 'Unknown'
        GROUP BY contract_address ORDER BY cnt DESC LIMIT 10
    """)
    for r in rows:
        addr = r[0][:12] + '...' + r[0][-6:] if r[0] else 'NULL'
        print(f"  {addr:20s} {r[1]:>12,}")

    # ── Check daily_txs query to diagnose blank charts ──
    print("\n" + "=" * 70)
    print("BLANK CHART DIAGNOSIS")
    print("=" * 70)
    print("\n  Checking transactions.timestamp format...")
    rows = neon.query("SELECT timestamp FROM transactions WHERE timestamp IS NOT NULL LIMIT 5")
    for r in rows:
        print(f"    Sample: {r[0]} (type: {type(r[0]).__name__})")

    print("\n  Checking if daily_txs query returns data...")
    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*) as cnt
        FROM transactions
        WHERE timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1 LIMIT 5
    """)
    if rows:
        for r in rows:
            print(f"    {r[0]} → {r[1]:,} txs")
    else:
        print("    NO DATA RETURNED — this is why charts are blank!")
        print("    Trying without date filter...")
        rows = neon.query("""
            SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*) as cnt
            FROM transactions
            WHERE timestamp IS NOT NULL
            GROUP BY 1 ORDER BY 1 LIMIT 5
        """)
        if rows:
            for r in rows:
                print(f"    {r[0]} → {r[1]:,} txs")
        else:
            print("    Still no data — timestamp column may be NULL or unparseable")
            rows = neon.query("SELECT COUNT(*) FROM transactions WHERE timestamp IS NOT NULL")
            print(f"    Transactions with non-NULL timestamp: {rows[0][0]:,}")
            rows = neon.query("SELECT COUNT(*) FROM transactions")
            print(f"    Total transactions: {rows[0][0]:,}")

    neon.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
