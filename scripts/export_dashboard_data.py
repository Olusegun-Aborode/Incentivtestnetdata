#!/usr/bin/env python3
"""
Export analytics data from Neon to JSON files for the static HTML dashboard.

The dashboard reads these JSON files to render charts without needing
a backend server or direct database connection.

Usage:
    python scripts/export_dashboard_data.py
    python scripts/export_dashboard_data.py --output-dir dashboards/data
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.loaders.neon import NeonLoader


def export_data(neon: NeonLoader, output_dir: Path) -> None:
    """Export all dashboard data to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Chain overview
    counts = neon.get_table_counts()
    block_range = neon.query(
        "SELECT MIN(number), MAX(number), MIN(timestamp)::text, MAX(timestamp)::text "
        "FROM blocks WHERE timestamp IS NOT NULL"
    )
    overview = {
        "table_counts": counts,
        "block_range": {
            "min": block_range[0][0] if block_range else 0,
            "max": block_range[0][1] if block_range else 0,
            "first_timestamp": block_range[0][2] if block_range else None,
            "last_timestamp": block_range[0][3] if block_range else None,
        },
        "exported_at": datetime.utcnow().isoformat(),
    }
    _write_json(output_dir / "overview.json", overview)
    print("  overview.json")

    # 2. Daily transactions
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day, COUNT(*) AS tx_count
        FROM transactions WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)
    _write_df_json(output_dir / "daily_transactions.json", df)
    print("  daily_transactions.json")

    # 3. Daily active addresses
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day,
               COUNT(DISTINCT from_address) AS senders,
               COUNT(DISTINCT to_address) AS receivers
        FROM transactions WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)
    _write_df_json(output_dir / "daily_addresses.json", df)
    print("  daily_addresses.json")

    # 4. Daily gas usage
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day,
               SUM(gas_used)::bigint AS total_gas,
               AVG(gas_used)::bigint AS avg_gas,
               COUNT(*) AS block_count
        FROM blocks WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)
    _write_df_json(output_dir / "daily_gas.json", df)
    print("  daily_gas.json")

    # 5. Event distribution
    df = neon.query_df("""
        SELECT event_name, COUNT(*) AS count
        FROM decoded_events GROUP BY event_name ORDER BY count DESC LIMIT 20
    """)
    _write_df_json(output_dir / "event_distribution.json", df)
    print("  event_distribution.json")

    # 6. Top contracts
    df = neon.query_df("""
        SELECT address, COUNT(*) AS log_count,
               COUNT(DISTINCT topic0) AS unique_events,
               MIN(block_number) AS first_block,
               MAX(block_number) AS last_block
        FROM raw_logs GROUP BY address ORDER BY log_count DESC LIMIT 20
    """)
    _write_df_json(output_dir / "top_contracts.json", df)
    print("  top_contracts.json")

    # 7. Bridge activity
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day,
               event_name, COUNT(*) AS count
        FROM decoded_events
        WHERE event_name IN ('SentTransferRemote', 'ReceivedTransferRemote')
          AND timestamp IS NOT NULL
        GROUP BY day, event_name ORDER BY day
    """)
    _write_df_json(output_dir / "bridge_activity.json", df)
    print("  bridge_activity.json")

    # 8. DEX activity
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day,
               event_name, COUNT(*) AS count
        FROM decoded_events
        WHERE event_name IN ('Swap', 'Mint', 'Burn')
          AND timestamp IS NOT NULL
        GROUP BY day, event_name ORDER BY day
    """)
    _write_df_json(output_dir / "dex_activity.json", df)
    print("  dex_activity.json")

    # 9. Hourly transaction distribution (for heatmap)
    df = neon.query_df("""
        SELECT EXTRACT(DOW FROM timestamp)::int AS dow,
               EXTRACT(HOUR FROM timestamp)::int AS hour,
               COUNT(*) AS count
        FROM transactions
        WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY dow, hour ORDER BY dow, hour
    """)
    _write_df_json(output_dir / "hourly_heatmap.json", df)
    print("  hourly_heatmap.json")


def _write_json(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, default=str, indent=2)


def _write_df_json(path: Path, df: pd.DataFrame) -> None:
    if df.empty:
        _write_json(path, [])
        return
    records = df.to_dict(orient="records")
    _write_json(path, records)


def main():
    parser = argparse.ArgumentParser(description="Export Neon data for dashboard")
    parser.add_argument("--output-dir", default="dashboards/data", help="Output directory")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    print("Connecting to Neon...")
    neon = NeonLoader()

    print(f"Exporting dashboard data to {output_dir}/")
    try:
        export_data(neon, output_dir)
        print(f"\nDone! Data exported to {output_dir}/")
    finally:
        neon.close()


if __name__ == "__main__":
    main()
