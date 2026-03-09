#!/usr/bin/env python3
"""
Quick data snapshot — shows what's in Neon right now.
Run anytime to check progress and available metrics.

Usage: python3 scripts/data_snapshot.py
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
    print("  INCENTIV BLOCKCHAIN — DATA SNAPSHOT")
    print("=" * 70)

    # 1. Table counts
    print("\n📊 TABLE COUNTS")
    print("-" * 50)
    counts = neon.get_table_counts()
    for t, c in counts.items():
        print(f"  {t:20s} {c:>12,}")

    # 2. Block range
    print("\n📦 BLOCK RANGE")
    print("-" * 50)
    rows = neon.query("SELECT MIN(number), MAX(number) FROM blocks")
    if rows and rows[0][0] is not None:
        mn, mx = rows[0]
        print(f"  Min block:  {mn:,}")
        print(f"  Max block:  {mx:,}")
        print(f"  Span:       {mx - mn:,} blocks")

    # 3. Date range
    print("\n📅 DATE RANGE")
    print("-" * 50)
    rows = neon.query("SELECT MIN(timestamp), MAX(timestamp) FROM blocks WHERE timestamp IS NOT NULL")
    if rows and rows[0][0] is not None:
        print(f"  Earliest:   {rows[0][0]}")
        print(f"  Latest:     {rows[0][1]}")

    # 4. Extraction state
    print("\n⚙️  EXTRACTION STATE")
    print("-" * 50)
    state = neon.get_extraction_state("all_activity")
    print(f"  Last block: {state['last_block_processed']:,}")
    print(f"  Status:     {state['status']}")
    print(f"  Updated:    {state.get('updated_at', 'N/A')}")

    # 5. Chain tip estimate
    try:
        from src.config import load_yaml
        from src.extractors.blockscout import BlockscoutExtractor
        chains = load_yaml("config/chains.yaml")
        cfg = chains["incentiv"]
        ext = BlockscoutExtractor(
            base_url=cfg["blockscout_base_url"],
            rpc_url=cfg["blockscout_rpc_url"],
            confirmations=int(cfg["confirmations"]),
            batch_size=50,
            rate_limit_per_second=float(cfg["rate_limit_per_second"]),
        )
        tip = ext.get_latest_block_number()
        last = state['last_block_processed']
        remaining = tip - last
        print(f"\n  Chain tip:  {tip:,}")
        print(f"  Remaining:  {remaining:,} blocks")
        # ETA at ~1.1 blocks/sec
        eta_hours = remaining / 1.1 / 3600
        print(f"  ETA (~1.1 b/s): {eta_hours:.1f} hours")
    except Exception:
        pass

    # 6. Top decoded events
    print("\n🔍 TOP DECODED EVENT TYPES")
    print("-" * 50)
    rows = neon.query("""
        SELECT event_name, COUNT(*) as cnt
        FROM decoded_events
        GROUP BY event_name
        ORDER BY cnt DESC
        LIMIT 15
    """)
    for name, cnt in rows:
        print(f"  {name:40s} {cnt:>10,}")

    # 7. Top contracts
    print("\n📜 TOP CONTRACTS (by event count)")
    print("-" * 50)
    rows = neon.query("""
        SELECT address, event_count
        FROM contracts
        ORDER BY event_count DESC
        LIMIT 10
    """)
    for addr, cnt in rows:
        print(f"  {addr}  {cnt:>10,}")

    # 8. Unique addresses
    print("\n👥 UNIQUE ADDRESSES")
    print("-" * 50)
    rows = neon.query("SELECT COUNT(DISTINCT from_address) FROM transactions")
    print(f"  Unique senders:    {rows[0][0]:,}")
    rows = neon.query("SELECT COUNT(DISTINCT to_address) FROM transactions")
    print(f"  Unique receivers:  {rows[0][0]:,}")

    # 9. Recent daily transaction volume
    print("\\n📈 DAILY TRANSACTION VOLUME (recent)")
    print("-" * 50)
    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*) as tx_count
        FROM transactions
        WHERE timestamp IS NOT NULL
        GROUP BY DATE_TRUNC('day', timestamp::TIMESTAMPTZ)
        ORDER BY day DESC
        LIMIT 10
    """)
    for day, cnt in rows:
        print(f"  {day.strftime('%Y-%m-%d'):12s} {cnt:>10,} txs")

    # 10. Daily event volume
    print("\\n📊 DAILY EVENT VOLUME (recent)")
    print("-" * 50)
    rows = neon.query("""
        SELECT DATE_TRUNC('day', block_timestamp::TIMESTAMPTZ) as day, COUNT(*) as log_count
        FROM raw_logs
        WHERE block_timestamp IS NOT NULL
        GROUP BY DATE_TRUNC('day', block_timestamp::TIMESTAMPTZ)
        ORDER BY day DESC
        LIMIT 10
    """)
    for day, cnt in rows:
        print(f"  {day.strftime('%Y-%m-%d'):12s} {cnt:>10,} events")

    print(f"\n{'=' * 70}")
    neon.close()


if __name__ == "__main__":
    main()
