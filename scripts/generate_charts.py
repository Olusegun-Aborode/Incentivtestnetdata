#!/usr/bin/env python3
"""
Generate analytics charts from Neon database.

Produces publication-quality static charts (PNG) and interactive Plotly HTML charts
for Incentiv blockchain metrics.

Usage:
    python scripts/generate_charts.py                    # All charts
    python scripts/generate_charts.py --output-dir charts  # Custom output dir
    python scripts/generate_charts.py --interactive       # Also generate interactive HTML
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.loaders.neon import NeonLoader


def setup_style():
    """Configure matplotlib style for clean, professional charts."""
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams.update({
        'figure.figsize': (12, 6),
        'figure.dpi': 150,
        'font.size': 11,
        'axes.titlesize': 14,
        'axes.labelsize': 12,
        'legend.fontsize': 10,
        'figure.facecolor': 'white',
        'axes.facecolor': '#fafafa',
        'grid.alpha': 0.3,
    })


# ============================================================
# Chart generators
# ============================================================

def chart_daily_transactions(neon: NeonLoader, output_dir: Path) -> None:
    """Daily transaction count over time."""
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp) AS day,
               COUNT(*) AS tx_count
        FROM transactions
        WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)
    if df.empty:
        print("  No transaction data for daily chart")
        return

    fig, ax = plt.subplots()
    ax.fill_between(df['day'], df['tx_count'], alpha=0.3, color='#2563eb')
    ax.plot(df['day'], df['tx_count'], color='#2563eb', linewidth=1.5)
    ax.set_title('Daily Transactions on Incentiv')
    ax.set_xlabel('Date')
    ax.set_ylabel('Transaction Count')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'daily_transactions.png')
    plt.close()
    print("  daily_transactions.png")


def chart_daily_active_addresses(neon: NeonLoader, output_dir: Path) -> None:
    """Daily unique sender and receiver addresses."""
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp) AS day,
               COUNT(DISTINCT from_address) AS senders,
               COUNT(DISTINCT to_address) AS receivers
        FROM transactions
        WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)
    if df.empty:
        print("  No address data for daily chart")
        return

    fig, ax = plt.subplots()
    ax.plot(df['day'], df['senders'], label='Unique Senders', color='#2563eb', linewidth=1.5)
    ax.plot(df['day'], df['receivers'], label='Unique Receivers', color='#dc2626', linewidth=1.5)
    ax.fill_between(df['day'], df['senders'], alpha=0.15, color='#2563eb')
    ax.fill_between(df['day'], df['receivers'], alpha=0.15, color='#dc2626')
    ax.set_title('Daily Active Addresses')
    ax.set_xlabel('Date')
    ax.set_ylabel('Unique Addresses')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'daily_active_addresses.png')
    plt.close()
    print("  daily_active_addresses.png")


def chart_gas_usage(neon: NeonLoader, output_dir: Path) -> None:
    """Daily gas usage and gas limit utilization."""
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp) AS day,
               SUM(gas_used) AS total_gas,
               AVG(gas_used) AS avg_gas,
               COUNT(*) AS block_count
        FROM blocks
        WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)
    if df.empty:
        print("  No gas data for chart")
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    ax1.bar(df['day'], df['total_gas'] / 1e9, color='#f59e0b', alpha=0.7, width=0.8)
    ax1.set_title('Daily Total Gas Used (Ggas)')
    ax1.set_ylabel('Gas (Ggas)')

    ax2.plot(df['day'], df['avg_gas'] / 1e6, color='#10b981', linewidth=1.5)
    ax2.fill_between(df['day'], df['avg_gas'] / 1e6, alpha=0.2, color='#10b981')
    ax2.set_title('Average Gas per Block (Mgas)')
    ax2.set_ylabel('Gas (Mgas)')
    ax2.set_xlabel('Date')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'gas_usage.png')
    plt.close()
    print("  gas_usage.png")


def chart_event_distribution(neon: NeonLoader, output_dir: Path) -> None:
    """Pie chart of decoded event types."""
    df = neon.query_df("""
        SELECT event_name, COUNT(*) AS count
        FROM decoded_events
        GROUP BY event_name
        ORDER BY count DESC
        LIMIT 10
    """)
    if df.empty:
        print("  No decoded events for distribution chart")
        return

    colors = ['#2563eb', '#dc2626', '#f59e0b', '#10b981', '#8b5cf6',
              '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1']

    fig, ax = plt.subplots(figsize=(10, 8))
    wedges, texts, autotexts = ax.pie(
        df['count'], labels=df['event_name'],
        colors=colors[:len(df)],
        autopct='%1.1f%%', pctdistance=0.85,
        startangle=90
    )
    for autotext in autotexts:
        autotext.set_fontsize(9)
    ax.set_title('Event Type Distribution')
    plt.tight_layout()
    plt.savefig(output_dir / 'event_distribution.png')
    plt.close()
    print("  event_distribution.png")


def chart_top_contracts(neon: NeonLoader, output_dir: Path) -> None:
    """Top contracts by event count (horizontal bar)."""
    df = neon.query_df("""
        SELECT address, COUNT(*) AS log_count
        FROM raw_logs
        GROUP BY address
        ORDER BY log_count DESC
        LIMIT 15
    """)
    if df.empty:
        print("  No raw logs for top contracts chart")
        return

    # Shorten addresses for display
    df['short_addr'] = df['address'].apply(lambda x: f"{x[:8]}...{x[-6:]}" if len(x) > 14 else x)

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['short_addr'][::-1], df['log_count'][::-1], color='#6366f1', alpha=0.8)
    ax.set_title('Top 15 Contracts by Event Count')
    ax.set_xlabel('Number of Events')
    for bar, count in zip(bars, df['log_count'][::-1]):
        ax.text(bar.get_width() + max(df['log_count']) * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f'{count:,}', va='center', fontsize=9)
    plt.tight_layout()
    plt.savefig(output_dir / 'top_contracts.png')
    plt.close()
    print("  top_contracts.png")


def chart_bridge_volume(neon: NeonLoader, output_dir: Path) -> None:
    """Warp route bridge transfer volumes over time."""
    df = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp) AS day,
               event_name,
               COUNT(*) AS transfer_count
        FROM decoded_events
        WHERE event_name IN ('SentTransferRemote', 'ReceivedTransferRemote')
          AND timestamp IS NOT NULL
        GROUP BY day, event_name
        ORDER BY day
    """)
    if df.empty:
        print("  No bridge data for volume chart")
        return

    fig, ax = plt.subplots()
    for event_name, group in df.groupby('event_name'):
        color = '#2563eb' if 'Sent' in event_name else '#10b981'
        label = 'Outbound' if 'Sent' in event_name else 'Inbound'
        ax.bar(group['day'], group['transfer_count'], alpha=0.7,
               label=label, color=color, width=0.8)
    ax.set_title('Daily Bridge Transfers (Warp Routes)')
    ax.set_xlabel('Date')
    ax.set_ylabel('Transfer Count')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'bridge_volume.png')
    plt.close()
    print("  bridge_volume.png")


def chart_chain_overview(neon: NeonLoader, output_dir: Path) -> None:
    """Summary stats card as a chart."""
    counts = neon.get_table_counts()

    block_range = neon.query("SELECT MIN(number), MAX(number), MIN(timestamp), MAX(timestamp) FROM blocks WHERE timestamp IS NOT NULL")
    if block_range and block_range[0][0] is not None:
        min_block, max_block, first_ts, last_ts = block_range[0]
    else:
        min_block, max_block, first_ts, last_ts = 0, 0, "N/A", "N/A"

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.axis('off')

    stats = [
        ("Total Blocks", f"{counts.get('blocks', 0):,}"),
        ("Total Transactions", f"{counts.get('transactions', 0):,}"),
        ("Total Raw Logs", f"{counts.get('raw_logs', 0):,}"),
        ("Decoded Events", f"{counts.get('decoded_events', 0):,}"),
        ("Contracts Discovered", f"{counts.get('contracts', 0):,}"),
        ("Block Range", f"{min_block:,} - {max_block:,}"),
        ("Time Range", f"{first_ts} to {last_ts}"),
    ]

    y = 0.9
    ax.text(0.5, 0.97, 'Incentiv Chain Overview', fontsize=18, fontweight='bold',
            ha='center', va='top', transform=ax.transAxes)

    for label, value in stats:
        ax.text(0.3, y, label, fontsize=13, ha='right', va='center',
                transform=ax.transAxes, color='#666')
        ax.text(0.35, y, value, fontsize=13, ha='left', va='center',
                transform=ax.transAxes, fontweight='bold', color='#1a1a1a')
        y -= 0.11

    plt.tight_layout()
    plt.savefig(output_dir / 'chain_overview.png')
    plt.close()
    print("  chain_overview.png")


# ============================================================
# Interactive Plotly charts (optional)
# ============================================================

def generate_interactive_dashboard(neon: NeonLoader, output_dir: Path) -> None:
    """Generate a single-file interactive HTML dashboard with Plotly."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Fetch data
    daily_txs = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day, COUNT(*) AS tx_count
        FROM transactions WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)

    daily_addrs = neon.query_df("""
        SELECT DATE_TRUNC('day', timestamp)::date AS day,
               COUNT(DISTINCT from_address) AS senders
        FROM transactions WHERE timestamp IS NOT NULL AND timestamp > '2025-09-01'
        GROUP BY day ORDER BY day
    """)

    events = neon.query_df("""
        SELECT event_name, COUNT(*) AS count
        FROM decoded_events GROUP BY event_name ORDER BY count DESC LIMIT 10
    """)

    top_contracts = neon.query_df("""
        SELECT address, COUNT(*) AS count
        FROM raw_logs GROUP BY address ORDER BY count DESC LIMIT 10
    """)

    counts = neon.get_table_counts()

    # Build dashboard
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            'Daily Transactions', 'Daily Active Senders',
            'Event Distribution', 'Top Contracts',
            'Chain Summary', ''
        ),
        specs=[
            [{"type": "scatter"}, {"type": "scatter"}],
            [{"type": "pie"}, {"type": "bar"}],
            [{"type": "table", "colspan": 2}, None],
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # Daily transactions
    if not daily_txs.empty:
        fig.add_trace(go.Scatter(
            x=daily_txs['day'], y=daily_txs['tx_count'],
            fill='tozeroy', fillcolor='rgba(37,99,235,0.15)',
            line=dict(color='#2563eb', width=2),
            name='Transactions'
        ), row=1, col=1)

    # Daily active senders
    if not daily_addrs.empty:
        fig.add_trace(go.Scatter(
            x=daily_addrs['day'], y=daily_addrs['senders'],
            fill='tozeroy', fillcolor='rgba(16,185,129,0.15)',
            line=dict(color='#10b981', width=2),
            name='Active Senders'
        ), row=1, col=2)

    # Event distribution pie
    if not events.empty:
        fig.add_trace(go.Pie(
            labels=events['event_name'], values=events['count'],
            hole=0.4, textinfo='label+percent',
            marker=dict(colors=['#2563eb', '#dc2626', '#f59e0b', '#10b981',
                                '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16',
                                '#f97316', '#6366f1'])
        ), row=2, col=1)

    # Top contracts bar
    if not top_contracts.empty:
        short = top_contracts['address'].apply(lambda x: f"{x[:6]}...{x[-4:]}")
        fig.add_trace(go.Bar(
            x=top_contracts['count'], y=short,
            orientation='h', marker_color='#6366f1',
            name='Events'
        ), row=2, col=2)

    # Summary table
    fig.add_trace(go.Table(
        header=dict(values=['Metric', 'Value'],
                    fill_color='#2563eb', font=dict(color='white', size=13),
                    align='left'),
        cells=dict(values=[
            ['Blocks', 'Transactions', 'Raw Logs', 'Decoded Events', 'Contracts'],
            [f"{counts.get(k, 0):,}" for k in ['blocks', 'transactions', 'raw_logs', 'decoded_events', 'contracts']]
        ], fill_color='#f8fafc', align='left', font=dict(size=12))
    ), row=3, col=1)

    fig.update_layout(
        title_text='Incentiv Blockchain Analytics Dashboard',
        title_font_size=20,
        height=1200,
        showlegend=False,
        template='plotly_white',
    )

    output_path = output_dir / 'incentiv_dashboard.html'
    fig.write_html(str(output_path), include_plotlyjs='cdn')
    print(f"  Interactive dashboard: {output_path}")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Generate Incentiv analytics charts")
    parser.add_argument("--output-dir", default="dashboards/charts", help="Output directory for charts")
    parser.add_argument("--interactive", action="store_true", help="Also generate interactive Plotly HTML dashboard")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_style()

    print("Connecting to Neon...")
    neon = NeonLoader()

    print(f"\nGenerating charts to {output_dir}/")

    try:
        chart_chain_overview(neon, output_dir)
        chart_daily_transactions(neon, output_dir)
        chart_daily_active_addresses(neon, output_dir)
        chart_gas_usage(neon, output_dir)
        chart_event_distribution(neon, output_dir)
        chart_top_contracts(neon, output_dir)
        chart_bridge_volume(neon, output_dir)

        if args.interactive:
            print("\nGenerating interactive dashboard...")
            generate_interactive_dashboard(neon, output_dir.parent)

        print(f"\nDone! Charts saved to {output_dir}/")
    except Exception as e:
        print(f"Error generating charts: {e}")
        raise
    finally:
        neon.close()


if __name__ == "__main__":
    main()
