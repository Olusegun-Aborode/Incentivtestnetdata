#!/usr/bin/env python3
"""
Generates a 5-tab interactive HTML dashboard from Neon DB data.
Now loads enrichment.json to display real token names instead of hex addresses.

Tabs: Overview | Swaps | Bridge | Transfers | Contracts

Usage: python3 scripts/generate_dashboard.py
Output: dashboard/incentiv_dashboard.html
"""
import json
import os
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader


# ── ENRICHMENT LOADER ───────────────────────────────────────────
def load_enrichment():
    """Load enrichment.json if available, return address->info map."""
    enrich_path = Path("dashboard/enrichment.json")
    if not enrich_path.exists():
        print("  [WARN] dashboard/enrichment.json not found. Run enrich_contracts.py first.")
        return {}
    data = json.loads(enrich_path.read_text(encoding="utf-8"))
    return data.get("contracts", {})


def get_display_name(address, enrichment):
    """Get human-readable name for an address from enrichment data."""
    addr_lower = address.lower() if address else ""
    info = enrichment.get(addr_lower) or enrichment.get(address) or {}
    if info.get("token_name") and info.get("token_symbol"):
        return f"{info['token_name']} ({info['token_symbol']})"
    if info.get("contract_name"):
        return info["contract_name"]
    if info.get("address_name"):
        return info["address_name"]
    return None


# Well-known chain IDs for bridge display
CHAIN_NAMES = {
    "1": "Ethereum",
    "8453": "Base",
    "42161": "Arbitrum",
    "137": "Polygon",
    "10": "Optimism",
    "56": "BSC",
    "43114": "Avalanche",
    "250": "Fantom",
    "324": "zkSync Era",
    "59144": "Linea",
    "534352": "Scroll",
    "1101": "Polygon zkEVM",
    "24125": "Incentiv",
}


# ── DATA QUERIES ─────────────────────────────────────────────────
def query_overview(neon):
    d = {}
    print("  [Overview] Table counts...")
    d["counts"] = neon.get_table_counts()

    print("  [Overview] Block range...")
    rows = neon.query("SELECT MIN(number), MAX(number) FROM blocks")
    d["block_min"] = rows[0][0] or 0
    d["block_max"] = rows[0][1] or 0

    print("  [Overview] Blockscout KPIs...")
    rows = neon.query("SELECT EXTRACT(EPOCH FROM (MAX(timestamp::TIMESTAMPTZ) - MIN(timestamp::TIMESTAMPTZ))) / GREATEST(COUNT(*), 1) FROM blocks WHERE timestamp IS NOT NULL")
    d["avg_block_time_s"] = rows[0][0] if rows and rows[0][0] else 0

    rows = neon.query("SELECT COUNT(*) FROM decoded_events WHERE event_name = 'Transfer'")
    d["total_cent_transfers"] = rows[0][0] or 0

    rows = neon.query("SELECT COUNT(DISTINCT contract_address) FROM decoded_events WHERE event_name = 'Transfer'")
    d["total_tokens"] = rows[0][0] or 0

    # ERC-4337 UserOperation metrics (from decoded EntryPoint events)
    print("  [Overview] ERC-4337 UserOps...")
    rows = neon.query("SELECT COUNT(*) FROM decoded_events WHERE event_name = 'UserOperationEvent'")
    d["total_user_ops"] = rows[0][0] or 0

    rows = neon.query("SELECT COUNT(DISTINCT params->>'sender') FROM decoded_events WHERE event_name = 'UserOperationEvent' AND params IS NOT NULL")
    d["total_aa_wallets"] = rows[0][0] or 0

    rows = neon.query("SELECT COUNT(DISTINCT params->>'paymaster') FROM decoded_events WHERE event_name = 'UserOperationEvent' AND params IS NOT NULL AND params->>'paymaster' != '0x0000000000000000000000000000000000000000'")
    d["total_paymasters"] = rows[0][0] or 0

    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day,
               COUNT(*) as cnt,
               COUNT(DISTINCT params->>'sender') as wallets,
               COUNT(DISTINCT params->>'paymaster') as paymasters
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'
          AND timestamp IS NOT NULL
          AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["daily_userops"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": int(r[1]), "wallets": int(r[2]), "paymasters": int(r[3])} for r in rows if r[0]]

    # Compute cumulative user ops
    cum = 0
    for day in d["daily_userops"]:
        cum += day["count"]
        day["cum_userops"] = cum

    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day,
               COUNT(DISTINCT params->>'sender') as bundlers
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'
          AND timestamp IS NOT NULL
          AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["daily_bundlers"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": int(r[1])} for r in rows if r[0]]

    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day,
               COUNT(DISTINCT params->>'paymaster') as paymasters
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'
          AND params->>'paymaster' != '0x0000000000000000000000000000000000000000'
          AND timestamp IS NOT NULL
          AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["daily_paymasters"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": int(r[1])} for r in rows if r[0]]

    rows = neon.query("SELECT COUNT(*), SUM(transactions.gas_used * COALESCE(CAST(NULLIF(gas_price, '') AS numeric), base_fee_per_gas, 0))/1e18 FROM transactions JOIN blocks ON transactions.block_number = blocks.number WHERE transactions.timestamp::TIMESTAMPTZ >= NOW() - INTERVAL '1 day'")
    d["tx_24h"] = rows[0][0] or 0
    d["tx_fees_24h"] = rows[0][1] or 0

    rows = neon.query("SELECT COUNT(DISTINCT from_address) FROM transactions")
    d["unique_senders"] = rows[0][0] or 0
    rows = neon.query("SELECT COUNT(DISTINCT to_address) FROM transactions")
    d["unique_receivers"] = rows[0][0] or 0

    # Top event types for doughnut chart
    print("  [Overview] Event type breakdown...")
    rows = neon.query("""
        SELECT event_name, COUNT(*) as cnt
        FROM decoded_events
        GROUP BY event_name ORDER BY cnt DESC LIMIT 10
    """)
    d["event_types"] = [{"name": r[0], "count": r[1]} for r in rows]

    print("  [Overview] Blockscout Daily Aggregates...")
    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*), AVG(size), AVG(gas_limit), SUM(gas_used), AVG(COALESCE(base_fee_per_gas, 0))
        FROM blocks WHERE timestamp IS NOT NULL AND timestamp > '2025-11-01' GROUP BY 1 ORDER BY 1
    """)
    d["daily_blocks"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": int(r[1]), "avg_size": float(r[2] or 0), "avg_gas_limit": float(r[3] or 0), "sum_gas_used": float(r[4] or 0), "avg_base_fee": float(r[5] or 0)} for r in rows if r[0]]

    rows = neon.query("""
        SELECT t.day, COUNT(*) as cnt, AVG(t.fee) as avg_fee, SUM(t.fee) as sum_fee, COUNT(DISTINCT t.from_address) as acts FROM (
            SELECT DATE_TRUNC('day', tx.timestamp::TIMESTAMPTZ) as day, tx.from_address, (tx.gas_used * COALESCE(CAST(NULLIF(tx.gas_price, '') AS numeric), b.base_fee_per_gas, 0))/1e18 as fee
            FROM transactions tx LEFT JOIN blocks b ON tx.block_number = b.number
            WHERE tx.timestamp IS NOT NULL AND tx.timestamp > '2025-11-01'
        ) t GROUP BY t.day ORDER BY t.day
    """)
    d["daily_txs"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": int(r[1]), "avg_fee": float(r[2] or 0), "sum_fee": float(r[3] or 0), "accounts": int(r[4] or 0)} for r in rows if r[0]]

    rows = neon.query("""
        SELECT DATE_TRUNC('day', created_at::TIMESTAMPTZ) as day, COUNT(*)
        FROM contracts WHERE created_at IS NOT NULL GROUP BY 1 ORDER BY 1
    """)
    d["daily_contracts"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    # Calculating cumulatives locally
    cum_txs = 0; cum_accounts = 0; cum_contracts = 0
    for day in d["daily_txs"]:
        cum_txs += day["count"]
        cum_accounts += day["accounts"]
        day["cum_txs"] = cum_txs
        day["cum_accounts"] = cum_accounts
    for day in d["daily_blocks"]:
        day["utilization"] = day["sum_gas_used"] / (day["avg_gas_limit"] * day["count"]) if day["avg_gas_limit"] and day["count"] else 0
    for day in d["daily_contracts"]:
        cum_contracts += day["count"]
        day["cum_contracts"] = cum_contracts

    return d


def query_swaps(neon):
    d = {}
    print("  [Swaps] Total count...")
    rows = neon.query("SELECT COUNT(*) FROM decoded_events WHERE event_name = 'Swap'")
    d["total_swaps"] = rows[0][0] or 0

    print("  [Swaps] Daily swaps...")
    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Swap'
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["daily_swaps"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    print("  [Swaps] Top DEX pools...")
    rows = neon.query("""
        SELECT contract_address, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Swap'
        GROUP BY contract_address ORDER BY cnt DESC LIMIT 15
    """)
    d["top_pools"] = [{"address": r[0], "count": r[1]} for r in rows]

    print("  [Swaps] Hourly swap distribution...")
    rows = neon.query("""
        SELECT EXTRACT(HOUR FROM timestamp::TIMESTAMPTZ) as hour, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Swap'
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["hourly_swaps"] = [{"hour": int(r[0]), "count": r[1]} for r in rows if r[0] is not None]

    print("  [Swaps] Weekly swap trend...")
    rows = neon.query("""
        SELECT DATE_TRUNC('week', timestamp::TIMESTAMPTZ) as week, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Swap'
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["weekly_swaps"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    # Unique swap participants (from params.sender and params.recipient)
    print("  [Swaps] Unique swappers...")
    rows = neon.query("""
        SELECT COUNT(DISTINCT params->>'sender') as senders,
               COUNT(DISTINCT params->>'recipient') as recipients
        FROM decoded_events WHERE event_name = 'Swap' AND params IS NOT NULL
    """)
    d["unique_senders"] = rows[0][0] or 0
    d["unique_recipients"] = rows[0][1] or 0

    # Top swappers by count
    print("  [Swaps] Top swappers...")
    rows = neon.query("""
        SELECT params->>'sender' as sender, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Swap' AND params->>'sender' IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """)
    d["top_swappers"] = [{"address": r[0], "count": r[1]} for r in rows]

    return d


def query_bridge(neon):
    d = {}
    bridge_events = "('SentTransferRemote','ReceivedTransferRemote','Dispatch','DispatchId','Process','ProcessId')"

    print("  [Bridge] Total bridge events...")
    rows = neon.query(f"SELECT COUNT(*) FROM decoded_events WHERE event_name IN {bridge_events}")
    d["total_bridge"] = rows[0][0] or 0

    print("  [Bridge] Bridge event breakdown...")
    rows = neon.query(f"""
        SELECT event_name, COUNT(*) as cnt
        FROM decoded_events WHERE event_name IN {bridge_events}
        GROUP BY event_name ORDER BY cnt DESC
    """)
    d["bridge_breakdown"] = [{"name": r[0], "count": r[1]} for r in rows]

    print("  [Bridge] Daily bridge events...")
    rows = neon.query(f"""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*) as cnt
        FROM decoded_events WHERE event_name IN {bridge_events}
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["daily_bridge"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    print("  [Bridge] Bridge by direction...")
    rows = neon.query("""
        SELECT
            CASE WHEN event_name IN ('SentTransferRemote','Dispatch','DispatchId') THEN 'Outbound'
                 ELSE 'Inbound' END as direction,
            COUNT(*) as cnt
        FROM decoded_events WHERE event_name IN ('SentTransferRemote','ReceivedTransferRemote','Dispatch','DispatchId','Process','ProcessId')
        GROUP BY 1
    """)
    d["bridge_direction"] = [{"direction": r[0], "count": r[1]} for r in rows]

    print("  [Bridge] Bridge contracts...")
    rows = neon.query(f"""
        SELECT contract_address, COUNT(*) as cnt
        FROM decoded_events WHERE event_name IN {bridge_events}
        GROUP BY contract_address ORDER BY cnt DESC LIMIT 10
    """)
    d["bridge_contracts"] = [{"address": r[0], "count": r[1]} for r in rows]

    print("  [Bridge] Weekly bridge trend...")
    rows = neon.query(f"""
        SELECT DATE_TRUNC('week', timestamp::TIMESTAMPTZ) as week, COUNT(*) as cnt
        FROM decoded_events WHERE event_name IN {bridge_events}
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["weekly_bridge"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    # Chain destinations from params
    print("  [Bridge] Chain destinations from params...")
    rows = neon.query("""
        SELECT params->>'destination' as dest, COUNT(*) as cnt
        FROM decoded_events
        WHERE event_name IN ('SentTransferRemote','Dispatch')
        AND params->>'destination' IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """)
    d["bridge_destinations"] = [{"chain_id": r[0], "count": r[1]} for r in rows]

    rows = neon.query("""
        SELECT params->>'origin' as origin, COUNT(*) as cnt
        FROM decoded_events
        WHERE event_name IN ('ReceivedTransferRemote','Process')
        AND params->>'origin' IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """)
    d["bridge_origins"] = [{"chain_id": r[0], "count": r[1]} for r in rows]

    # Bridge tokens (which token contracts are used in bridge operations)
    print("  [Bridge] Bridge token breakdown...")
    rows = neon.query("""
        SELECT contract_address, COUNT(*) as cnt
        FROM decoded_events
        WHERE event_name IN ('SentTransferRemote','ReceivedTransferRemote')
        GROUP BY contract_address ORDER BY cnt DESC LIMIT 10
    """)
    d["bridge_tokens"] = [{"address": r[0], "count": r[1]} for r in rows]

    # Unique bridge users
    print("  [Bridge] Unique bridge users...")
    rows = neon.query("""
        SELECT COUNT(DISTINCT params->>'recipient') FROM decoded_events
        WHERE event_name IN ('SentTransferRemote','ReceivedTransferRemote')
        AND params->>'recipient' IS NOT NULL
    """)
    d["unique_bridge_users"] = rows[0][0] or 0

    return d


def query_transfers(neon):
    d = {}
    print("  [Transfers] Total count...")
    rows = neon.query("SELECT COUNT(*) FROM decoded_events WHERE event_name = 'Transfer'")
    d["total_transfers"] = rows[0][0] or 0

    print("  [Transfers] Daily transfers...")
    rows = neon.query("""
        SELECT DATE_TRUNC('day', timestamp::TIMESTAMPTZ) as day, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Transfer'
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["daily_transfers"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    print("  [Transfers] Top token contracts...")
    rows = neon.query("""
        SELECT contract_address, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Transfer'
        GROUP BY contract_address ORDER BY cnt DESC LIMIT 15
    """)
    d["top_tokens"] = [{"address": r[0], "count": r[1]} for r in rows]

    print("  [Transfers] Top senders...")
    rows = neon.query("""
        SELECT params->>'from' as sender, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Transfer' AND params->>'from' IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 15
    """)
    d["top_senders"] = [{"address": r[0], "count": r[1]} for r in rows]

    print("  [Transfers] Top receivers...")
    rows = neon.query("""
        SELECT params->>'to' as receiver, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Transfer' AND params->>'to' IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 15
    """)
    d["top_receivers"] = [{"address": r[0], "count": r[1]} for r in rows]

    print("  [Transfers] Weekly trend...")
    rows = neon.query("""
        SELECT DATE_TRUNC('week', timestamp::TIMESTAMPTZ) as week, COUNT(*) as cnt
        FROM decoded_events WHERE event_name = 'Transfer'
        AND timestamp IS NOT NULL AND timestamp > '2025-11-01'
        GROUP BY 1 ORDER BY 1
    """)
    d["weekly_transfers"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    print("  [Transfers] Unique transfer participants...")
    rows = neon.query("""
        SELECT COUNT(DISTINCT params->>'from') as senders,
               COUNT(DISTINCT params->>'to') as receivers
        FROM decoded_events WHERE event_name = 'Transfer' AND params IS NOT NULL
    """)
    d["unique_senders"] = rows[0][0] or 0
    d["unique_receivers"] = rows[0][1] or 0

    return d


def query_contracts(neon):
    d = {}
    print("  [Contracts] Total contracts...")
    rows = neon.query("SELECT COUNT(*) FROM contracts")
    d["total_contracts"] = rows[0][0] or 0

    print("  [Contracts] Top contracts...")
    rows = neon.query("""
        SELECT address, event_count, first_seen_block, last_activity_block
        FROM contracts ORDER BY event_count DESC LIMIT 20
    """)
    d["top_contracts"] = [
        {"address": r[0], "count": r[1], "first_block": r[2], "last_block": r[3]}
        for r in rows
    ]

    print("  [Contracts] Contracts by decoded event name...")
    rows = neon.query("""
        SELECT c.address, de.event_name, COUNT(*) as cnt
        FROM contracts c
        JOIN decoded_events de ON LOWER(c.address) = LOWER(de.contract_address)
        GROUP BY c.address, de.event_name
        ORDER BY cnt DESC
        LIMIT 30
    """)
    d["contract_events"] = [{"address": r[0], "event": r[1], "count": r[2]} for r in rows]

    print("  [Contracts] New contracts per week...")
    rows = neon.query("""
        SELECT DATE_TRUNC('week', created_at::TIMESTAMPTZ) as week, COUNT(*) as cnt
        FROM contracts WHERE created_at IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)
    d["weekly_new_contracts"] = [{"date": r[0].strftime("%Y-%m-%d"), "count": r[1]} for r in rows if r[0]]

    print("  [Contracts] Event diversity (contracts with multiple event types)...")
    rows = neon.query("""
        SELECT contract_address, COUNT(DISTINCT event_name) as event_types, COUNT(*) as total
        FROM decoded_events
        GROUP BY contract_address
        ORDER BY event_types DESC, total DESC
        LIMIT 15
    """)
    d["diverse_contracts"] = [{"address": r[0], "event_types": r[1], "total": r[2]} for r in rows]

    return d


# ── HTML GENERATION ──────────────────────────────────────────────
def generate_html(overview, swaps, bridge, transfers, contracts, enrichment):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    c = overview["counts"]

    # Build JS-side enrichment lookup map
    enrich_js = {}
    for addr, info in enrichment.items():
        name = get_display_name(addr, enrichment)
        if name:
            enrich_js[addr.lower()] = name

    # Build chain name lookup for bridge
    chain_names_js = json.dumps(CHAIN_NAMES)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Incentiv Blockchain Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.1"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background-color:hsl(222.2,84%,4.9%);background-image:radial-gradient(circle at 15% 50%,rgba(96,165,250,0.08),transparent 25%),radial-gradient(circle at 85% 30%,rgba(52,211,153,0.08),transparent 25%);color:hsl(210,40%,98%);min-height:100vh;padding-bottom:20px}}
.dashboard{{max-width:1440px;margin:0 auto;padding:20px 24px}}

/* Header */
header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;padding-bottom:12px;border-bottom:1px solid rgba(255,255,255,0.05)}}
header h1{{font-size:26px;font-weight:800;background:linear-gradient(to right,#60a5fa,#34d399);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.meta{{font-size:12px;color:hsl(215,20.2%,65.1%)}}
.meta .live{{display:inline-block;width:8px;height:8px;background:#34d399;border-radius:50%;margin-right:5px;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}

/* Tabs */
.tabs{{display:flex;gap:0;margin-bottom:24px;border-bottom:1px solid rgba(255,255,255,0.05)}}
.tab{{padding:12px 24px;font-size:14px;font-weight:600;color:hsl(215,20.2%,65.1%);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;user-select:none}}
.tab:hover{{color:#fff;background:rgba(255,255,255,0.02)}}
.tab.active{{color:#60a5fa;border-bottom-color:#60a5fa}}
.tab-content{{display:none}}
.tab-content.active{{display:block;animation:fadeIn 0.4s ease-out forwards}}
@keyframes fadeIn{{from{{opacity:0;transform:translateY(5px)}}to{{opacity:1;transform:translateY(0)}}}}

/* Glass Cards */
.glass{{background:rgba(255,255,255,0.03);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,0.08);border-radius:12px;box-shadow:0 4px 30px rgba(0,0,0,0.1)}}
.glass:hover{{border-color:rgba(255,255,255,0.15)}}

/* KPI Cards */
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}}
.kpi{{padding:20px;text-align:center;transition:transform .2s,box-shadow .2s}}
.kpi:hover{{transform:translateY(-4px);box-shadow:0 8px 30px rgba(96,165,250,0.1)}}
.kpi .value{{font-size:28px;font-weight:800;color:#fff;margin-bottom:4px;font-variant-numeric:tabular-nums}}
.kpi .label{{font-size:12px;color:hsl(215,20.2%,65.1%);text-transform:uppercase;letter-spacing:1px;font-weight:600}}
.kpi.blue .value{{color:#60a5fa}}.kpi.green .value{{color:#34d399}}.kpi.purple .value{{color:#c084fc}}
.kpi.orange .value{{color:#fbbf24}}.kpi.pink .value{{color:#f472b6}}.kpi.teal .value{{color:#2dd4bf}}

/* Chart grid */
.charts{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px}}
.chart-card{{padding:20px}}
.chart-card.full{{grid-column:1/-1}}
.chart-card h3{{font-size:15px;font-weight:600;color:#fff;margin-bottom:16px}}
.cc{{position:relative;width:100%;height:300px}}
.cc.tall{{height:380px}}

/* Table */
.tbl-card{{padding:20px;margin-bottom:24px;overflow-x:auto}}
.tbl-card h3{{font-size:15px;font-weight:600;color:#fff;margin-bottom:16px}}
table{{width:100%;border-collapse:collapse;white-space:nowrap}}
th{{text-align:left;padding:12px;font-size:12px;color:hsl(215,20.2%,65.1%);text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid rgba(255,255,255,0.05);font-weight:600}}
td{{padding:12px;font-size:14px;border-bottom:1px solid rgba(255,255,255,0.02);color:#e2e8f0}}
tr:hover td{{background:rgba(255,255,255,0.02)}}
.mono{{font-family:'SF Mono',SFMono-Regular,Consolas,monospace;font-size:13px;color:#60a5fa}}
.token-name{{color:#34d399;font-weight:600;font-size:14px}}
.token-badge{{display:inline-block;background:rgba(52,211,153,0.12);color:#34d399;padding:2px 8px;border-radius:6px;font-size:12px;font-weight:600;margin-left:6px}}
.chain-badge{{display:inline-block;background:rgba(96,165,250,0.12);color:#60a5fa;padding:2px 8px;border-radius:6px;font-size:12px;font-weight:600}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.bar-wrap{{display:flex;align-items:center;gap:12px}}
.bar-fill{{height:6px;border-radius:3px;background:linear-gradient(90deg,#60a5fa,#34d399)}}

footer{{text-align:center;padding:20px;font-size:12px;color:hsl(215,20.2%,65.1%)}}

@media(max-width:900px){{.charts{{grid-template-columns:1fr}}.kpis{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>
<div class="dashboard">
<header>
    <h1>Incentiv Blockchain Analytics</h1>
    <div class="meta"><span class="live"></span>Generated {ts} &middot; Blocks {overview['block_min']:,} &ndash; {overview['block_max']:,}</div>
</header>

<nav class="tabs">
    <div class="tab active" onclick="switchTab('overview')">Overview</div>
    <div class="tab" onclick="switchTab('swaps')">Swaps</div>
    <div class="tab" onclick="switchTab('bridge')">Bridge</div>
    <div class="tab" onclick="switchTab('transfers')">Transfers</div>
    <div class="tab" onclick="switchTab('contracts')">Contracts</div>
</nav>

<!-- ═══════════ OVERVIEW TAB ═══════════ -->
<div id="tab-overview" class="tab-content active">
    <section class="kpis">
        <div class="kpi glass blue"><div class="value">{c.get('blocks',0):,}</div><div class="label">Blocks</div></div>
        <div class="kpi glass green"><div class="value">{c.get('transactions',0):,}</div><div class="label">Transactions</div></div>
        <div class="kpi glass purple"><div class="value">{c.get('raw_logs',0):,}</div><div class="label">Event Logs</div></div>
        <div class="kpi glass orange"><div class="value">{c.get('decoded_events',0):,}</div><div class="label">Decoded Events</div></div>
        <div class="kpi glass pink"><div class="value">{c.get('contracts',0):,}</div><div class="label">Contracts</div></div>
        <div class="kpi glass teal"><div class="value">{overview['unique_senders']+overview['unique_receivers']:,}</div><div class="label">Unique Addresses</div></div>
    </section>
    <section class="charts">
        <div class="chart-card glass full"><h3>Daily Transactions</h3><div class="cc tall"><canvas id="ov_daily_txs"></canvas></div></div>
        <div class="chart-card glass"><h3>Event Type Breakdown</h3><div class="cc"><canvas id="ov_types"></canvas></div></div>
        <div class="chart-card glass"><h3>Daily Active Addresses</h3><div class="cc"><canvas id="ov_active_addr"></canvas></div></div>
        <div class="chart-card glass full"><h3>Network Gas Utilization</h3><div class="cc"><canvas id="ov_gas_util"></canvas></div></div>
        <div class="chart-card glass"><h3>Cumulative Transactions</h3><div class="cc"><canvas id="ov_cum_txs"></canvas></div></div>
        <div class="chart-card glass"><h3>Cumulative Contracts Discovered</h3><div class="cc"><canvas id="ov_cum_contracts"></canvas></div></div>
    </section>
</div>

<!-- ═══════════ SWAPS TAB ═══════════ -->
<div id="tab-swaps" class="tab-content">
    <section class="kpis">
        <div class="kpi glass blue"><div class="value">{swaps['total_swaps']:,}</div><div class="label">Total Swaps</div></div>
        <div class="kpi glass green"><div class="value">{len(swaps['top_pools'])}</div><div class="label">DEX Pools</div></div>
        <div class="kpi glass purple"><div class="value">{swaps['unique_senders']}</div><div class="label">Unique Swappers</div></div>
        <div class="kpi glass orange"><div class="value">{swaps['unique_recipients']}</div><div class="label">Unique Recipients</div></div>
    </section>
    <section class="charts">
        <div class="chart-card glass full"><h3>Daily Swap Volume</h3><div class="cc tall"><canvas id="sw_daily"></canvas></div></div>
        <div class="chart-card glass"><h3>Weekly Swap Trend</h3><div class="cc"><canvas id="sw_weekly"></canvas></div></div>
        <div class="chart-card glass"><h3>Hourly Swap Distribution (UTC)</h3><div class="cc"><canvas id="sw_hourly"></canvas></div></div>
    </section>
    <section class="tbl-card glass">
        <h3>DEX Pools</h3>
        <table><thead><tr><th>#</th><th>Pool</th><th>Contract</th><th style="text-align:right">Swap Count</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="sw_pools_tbl"></tbody></table>
    </section>
    <section class="tbl-card glass">
        <h3>Top Swappers</h3>
        <table><thead><tr><th>#</th><th>Address</th><th style="text-align:right">Swaps</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="sw_swappers_tbl"></tbody></table>
    </section>
</div>

<!-- ═══════════ BRIDGE TAB ═══════════ -->
<div id="tab-bridge" class="tab-content">
    <section class="kpis">
        <div class="kpi glass blue"><div class="value">{bridge['total_bridge']:,}</div><div class="label">Total Bridge Events</div></div>
        <div class="kpi glass green"><div class="value">{next((b['count'] for b in bridge['bridge_direction'] if b['direction']=='Inbound'), 0):,}</div><div class="label">Inbound</div></div>
        <div class="kpi glass purple"><div class="value">{next((b['count'] for b in bridge['bridge_direction'] if b['direction']=='Outbound'), 0):,}</div><div class="label">Outbound</div></div>
        <div class="kpi glass orange"><div class="value">{bridge.get('unique_bridge_users', 0):,}</div><div class="label">Unique Bridge Users</div></div>
    </section>
    <section class="charts">
        <div class="chart-card glass full"><h3>Daily Bridge Events</h3><div class="cc tall"><canvas id="br_daily"></canvas></div></div>
        <div class="chart-card glass"><h3>Bridge Event Types</h3><div class="cc"><canvas id="br_types"></canvas></div></div>
        <div class="chart-card glass"><h3>Weekly Bridge Trend</h3><div class="cc"><canvas id="br_weekly"></canvas></div></div>
    </section>
    <section class="tbl-card glass">
        <h3>Bridge Contracts</h3>
        <table><thead><tr><th>#</th><th>Name</th><th>Contract</th><th style="text-align:right">Events</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="br_contracts_tbl"></tbody></table>
    </section>
    <section class="tbl-card glass">
        <h3>Bridged Tokens</h3>
        <table><thead><tr><th>#</th><th>Token</th><th>Contract</th><th style="text-align:right">Bridge Txs</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="br_tokens_tbl"></tbody></table>
    </section>
    <section class="charts">
        <div class="chart-card glass"><h3>Source Chains (Inbound)</h3><div class="cc"><canvas id="br_origins"></canvas></div></div>
        <div class="chart-card glass"><h3>Destination Chains (Outbound)</h3><div class="cc"><canvas id="br_dests"></canvas></div></div>
    </section>
</div>

<!-- ═══════════ TRANSFERS TAB ═══════════ -->
<div id="tab-transfers" class="tab-content">
    <section class="kpis">
        <div class="kpi glass blue"><div class="value">{transfers['total_transfers']:,}</div><div class="label">Total Transfers</div></div>
        <div class="kpi glass green"><div class="value">{len(transfers['top_tokens'])}</div><div class="label">Token Contracts</div></div>
        <div class="kpi glass purple"><div class="value">{transfers['unique_senders']:,}</div><div class="label">Unique Senders</div></div>
        <div class="kpi glass orange"><div class="value">{transfers['unique_receivers']:,}</div><div class="label">Unique Receivers</div></div>
    </section>
    <section class="charts">
        <div class="chart-card glass full"><h3>Daily Transfer Volume</h3><div class="cc tall"><canvas id="tf_daily"></canvas></div></div>
        <div class="chart-card glass full"><h3>Weekly Transfer Trend</h3><div class="cc"><canvas id="tf_weekly"></canvas></div></div>
    </section>
    <section class="tbl-card glass">
        <h3>Top Tokens (by Transfer Count)</h3>
        <table><thead><tr><th>#</th><th>Token</th><th>Contract</th><th style="text-align:right">Transfers</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="tf_tokens_tbl"></tbody></table>
    </section>
    <section class="tbl-card glass">
        <h3>Top Senders</h3>
        <table><thead><tr><th>#</th><th>Address</th><th style="text-align:right">Transfers Sent</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="tf_senders_tbl"></tbody></table>
    </section>
    <section class="tbl-card glass">
        <h3>Top Receivers</h3>
        <table><thead><tr><th>#</th><th>Address</th><th style="text-align:right">Transfers Received</th><th style="width:240px">Share</th></tr></thead>
        <tbody id="tf_receivers_tbl"></tbody></table>
    </section>
</div>

<!-- ═══════════ CONTRACTS TAB ═══════════ -->
<div id="tab-contracts" class="tab-content">
    <section class="kpis">
        <div class="kpi glass blue"><div class="value">{contracts['total_contracts']:,}</div><div class="label">Total Contracts</div></div>
    </section>
    <section class="charts">
        <div class="chart-card glass full"><h3>New Contracts Discovered per Week</h3><div class="cc"><canvas id="ct_weekly"></canvas></div></div>
    </section>
    <section class="tbl-card glass">
        <h3>Top Contracts by Event Count</h3>
        <table><thead><tr><th>#</th><th>Name</th><th>Contract</th><th style="text-align:right">Events</th><th>First Block</th><th>Last Block</th><th style="width:200px">Share</th></tr></thead>
        <tbody id="ct_top_tbl"></tbody></table>
    </section>
    <section class="tbl-card glass">
        <h3>Most Diverse Contracts (Multiple Event Types)</h3>
        <table><thead><tr><th>#</th><th>Name</th><th>Contract</th><th style="text-align:right">Event Types</th><th style="text-align:right">Total Events</th></tr></thead>
        <tbody id="ct_diverse_tbl"></tbody></table>
    </section>
    <section class="tbl-card glass">
        <h3>Contract &times; Event Breakdown</h3>
        <table><thead><tr><th>Contract</th><th>Event</th><th style="text-align:right">Count</th></tr></thead>
        <tbody id="ct_events_tbl"></tbody></table>
    </section>
</div>

<footer>Incentiv Blockchain Analytics &middot; {ts} &middot; Data from Neon DB</footer>
</div>

<script>
// ═══════ ENRICHMENT MAP ═══════
const ENRICH = {json.dumps(enrich_js)};
const CHAIN_NAMES = {chain_names_js};

// ═══════ DATA ═══════
const OV_TXS = {json.dumps(overview.get('daily_txs',[]))};
const OV_BLOCKS = {json.dumps(overview.get('daily_blocks',[]))};
const OV_CONTRACTS = {json.dumps(overview.get('daily_contracts',[]))};
const OV_EVENT_TYPES = {json.dumps(overview.get('event_types',[]))};
const OV_USEROPS = {json.dumps(overview.get('daily_userops',[]))};
const OV_BUNDLERS = {json.dumps(overview.get('daily_bundlers',[]))};
const OV_PAYMASTERS = {json.dumps(overview.get('daily_paymasters',[]))};

const SW_DAILY = {json.dumps(swaps['daily_swaps'])};
const SW_WEEKLY = {json.dumps(swaps['weekly_swaps'])};
const SW_HOURLY = {json.dumps(swaps['hourly_swaps'])};
const SW_POOLS = {json.dumps(swaps['top_pools'])};
const SW_SWAPPERS = {json.dumps(swaps.get('top_swappers',[]))};

const BR_DAILY = {json.dumps(bridge['daily_bridge'])};
const BR_TYPES = {json.dumps(bridge['bridge_breakdown'])};
const BR_WEEKLY = {json.dumps(bridge['weekly_bridge'])};
const BR_CONTRACTS = {json.dumps(bridge['bridge_contracts'])};
const BR_TOKENS = {json.dumps(bridge.get('bridge_tokens',[]))};
const BR_ORIGINS = {json.dumps(bridge['bridge_origins'])};
const BR_DESTS = {json.dumps(bridge['bridge_destinations'])};

const TF_DAILY = {json.dumps(transfers['daily_transfers'])};
const TF_WEEKLY = {json.dumps(transfers['weekly_transfers'])};
const TF_TOKENS = {json.dumps(transfers['top_tokens'])};
const TF_SENDERS = {json.dumps(transfers['top_senders'])};
const TF_RECEIVERS = {json.dumps(transfers['top_receivers'])};

const CT_WEEKLY = {json.dumps(contracts['weekly_new_contracts'])};
const CT_TOP = {json.dumps(contracts['top_contracts'])};
const CT_DIVERSE = {json.dumps(contracts['diverse_contracts'])};
const CT_EVENTS = {json.dumps(contracts['contract_events'])};

// ═══════ GLOBALS ═══════
Chart.defaults.color = 'hsl(215, 20.2%, 65.1%)';
Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.05)';
Chart.defaults.font.family = "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
const COLORS = ['#60a5fa','#34d399','#c084fc','#fbbf24','#f472b6','#2dd4bf','#fb923c','#7dd3fc','#a3e635','#818cf8'];

function fmt(n) {{
    if (n >= 1e6) return (n/1e6).toFixed(1)+'M';
    if (n >= 1e3) return (n/1e3).toFixed(1)+'K';
    return n.toLocaleString();
}}
function shortAddr(a) {{ return a ? a.slice(0,8)+'...'+a.slice(-6) : '—'; }}
function enrichAddr(a) {{
    if (!a) return '—';
    const name = ENRICH[a.toLowerCase()];
    return name || null;
}}
function displayAddr(a) {{
    const name = enrichAddr(a);
    if (name) return `<span class="token-name">${{name}}</span><br><span class="mono" style="font-size:11px;opacity:0.6">${{shortAddr(a)}}</span>`;
    return `<span class="mono">${{shortAddr(a)}}</span>`;
}}
function displayAddrSimple(a) {{
    const name = enrichAddr(a);
    if (name) return `<span class="token-name">${{name}}</span>`;
    return `<span class="mono">${{shortAddr(a)}}</span>`;
}}
function chainName(id) {{
    return CHAIN_NAMES[String(id)] || ('Chain ' + id);
}}

// ═══════ TAB SWITCHING ═══════
let chartsInit = {{}};
function switchTab(name) {{
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.getElementById('tab-'+name).classList.add('active');
    event.target.classList.add('active');
    if (!chartsInit[name]) {{ chartsInit[name]=true; initCharts(name); }}
}}

// ═══════ CHART BUILDERS ═══════
function lineChart(id, labels, data, color, label) {{
    new Chart(document.getElementById(id), {{
        type:'line', data:{{ labels, datasets:[{{ label, data, borderColor:color, backgroundColor:color+'1a', fill:true, tension:.3, pointRadius:0, pointHoverRadius:5, borderWidth:2 }}] }},
        options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>fmt(c.raw)+' '+label}}}} }},
            scales:{{ x:{{grid:{{display:false}},ticks:{{maxTicksLimit:15,maxRotation:45}}}}, y:{{grid:{{color:'#21262d'}},ticks:{{callback:v=>fmt(v)}}}} }} }}
    }});
}}
function barChart(id, labels, data, color, label, horizontal) {{
    new Chart(document.getElementById(id), {{
        type:'bar', data:{{ labels, datasets:[{{ label, data, backgroundColor:color+'99', borderColor:color, borderWidth:1, borderRadius:3 }}] }},
        options:{{ indexAxis: horizontal?'y':'x', responsive:true, maintainAspectRatio:false,
            plugins:{{ legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>fmt(c.raw)+' '+label}}}} }},
            scales:{{ x:{{grid:{{display:horizontal?true:false,color:'#21262d'}},ticks:horizontal?{{callback:v=>fmt(v)}}:{{}}}}, y:{{grid:{{display:horizontal?false:true,color:'#21262d'}},ticks:horizontal?{{}}:{{callback:v=>fmt(v)}}}} }} }}
    }});
}}
function doughnutChart(id, labels, data) {{
    new Chart(document.getElementById(id), {{
        type:'doughnut', data:{{ labels, datasets:[{{ data, backgroundColor:COLORS.slice(0,data.length), borderWidth:0 }}] }},
        options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{position:'right',labels:{{boxWidth:12,padding:8,font:{{size:11}}}}}}, tooltip:{{callbacks:{{label:c=>c.label+': '+fmt(c.raw)}}}} }} }}
    }});
}}

// ═══════ TABLE BUILDERS (with enrichment) ═══════
function fillEnrichedTable(id, rows, maxVal, opts) {{
    const tbody = document.getElementById(id);
    if (!tbody) return;
    rows.forEach((r,i) => {{
        const pct = (r.count / maxVal * 100).toFixed(1);
        const tr = document.createElement('tr');
        const addr = r.address || '';
        const name = enrichAddr(addr);
        let cells = `<td style="color:#8b949e">${{i+1}}</td>`;
        if (opts && opts.showName) {{
            cells += `<td>${{name ? `<span class="token-name">${{name}}</span>` : '<span style="color:#8b949e">Unknown</span>'}}</td>`;
            cells += `<td class="mono" style="font-size:12px">${{shortAddr(addr)}}</td>`;
        }} else {{
            cells += `<td>${{displayAddr(addr)}}</td>`;
        }}
        if (opts && opts.extraCols) {{
            opts.extraCols.forEach(col => {{
                cells += `<td class="num">${{(r[col]||0).toLocaleString()}}</td>`;
            }});
        }}
        cells += `<td class="num">${{r.count.toLocaleString()}}</td>`;
        cells += `<td><div class="bar-wrap"><div class="bar-fill" style="width:${{pct}}%"></div><span style="font-size:11px;color:#8b949e">${{pct}}%</span></div></td>`;
        tr.innerHTML = cells;
        tbody.appendChild(tr);
    }});
}}

function fillAddressTable(id, rows, maxVal) {{
    const tbody = document.getElementById(id);
    if (!tbody) return;
    rows.forEach((r,i) => {{
        const pct = (r.count / maxVal * 100).toFixed(1);
        const tr = document.createElement('tr');
        const addr = r.address || '';
        tr.innerHTML = `<td style="color:#8b949e">${{i+1}}</td><td>${{displayAddr(addr)}}</td><td class="num">${{r.count.toLocaleString()}}</td><td><div class="bar-wrap"><div class="bar-fill" style="width:${{pct}}%"></div><span style="font-size:11px;color:#8b949e">${{pct}}%</span></div></td>`;
        tbody.appendChild(tr);
    }});
}}

// ═══════ INIT PER TAB ═══════
function initCharts(tab) {{
    if (tab === 'overview') {{
        // Event type doughnut
        if(OV_EVENT_TYPES.length) doughnutChart('ov_event_types', OV_EVENT_TYPES.map(e=>e.name), OV_EVENT_TYPES.map(e=>e.count));
        // Gas utilization
        if(OV_BLOCKS.length) lineChart('ov_gas', OV_BLOCKS.map(d=>d.date), OV_BLOCKS.map(d=>d.avg_gas_used||0), '#f0ad4e', 'avg gas used');
        // Cumulative contracts
        if(OV_CONTRACTS.length) lineChart('ov_cum_contracts', OV_CONTRACTS.map(d=>d.date), OV_CONTRACTS.map(d=>d.cum_count||d.count), '#bc8cff', 'contracts');
        // Daily transactions
        if(OV_TXS.length) lineChart('ov_daily_txs', OV_TXS.map(d=>d.date), OV_TXS.map(d=>d.count), '#60a5fa', 'transactions');
        // Active addresses
        if(OV_TXS.length) lineChart('ov_active_addrs', OV_TXS.map(d=>d.date), OV_TXS.map(d=>d.unique_senders||0), '#34d399', 'addresses');
        // ERC-4337 — cumulative UserOps
        if(OV_USEROPS.length) lineChart('ov_cum_userops', OV_USEROPS.map(d=>d.date), OV_USEROPS.map(d=>d.cum_userops||0), '#58a6ff', 'user ops');
        // ERC-4337 — new UserOps per day
        if(OV_USEROPS.length) barChart('ov_new_userops', OV_USEROPS.map(d=>d.date), OV_USEROPS.map(d=>d.count), '#7c3aed', 'user ops', false);
        // ERC-4337 — active bundlers per day
        if(OV_BUNDLERS.length) lineChart('ov_bundlers', OV_BUNDLERS.map(d=>d.date), OV_BUNDLERS.map(d=>d.count), '#f472b6', 'bundlers');
        // ERC-4337 — active paymasters per day
        if(OV_PAYMASTERS.length) lineChart('ov_paymasters', OV_PAYMASTERS.map(d=>d.date), OV_PAYMASTERS.map(d=>d.count), '#2dd4bf', 'paymasters');
    }}
    if (tab === 'swaps') {{
        lineChart('sw_daily', SW_DAILY.map(d=>d.date), SW_DAILY.map(d=>d.count), '#58a6ff', 'swaps');
        barChart('sw_weekly', SW_WEEKLY.map(d=>d.date), SW_WEEKLY.map(d=>d.count), '#3fb950', 'swaps', false);
        barChart('sw_hourly', SW_HOURLY.map(h=>h.hour+':00'), SW_HOURLY.map(h=>h.count), '#bc8cff', 'swaps', false);
        if(SW_POOLS.length) fillEnrichedTable('sw_pools_tbl', SW_POOLS, SW_POOLS[0].count, {{showName:true}});
        if(SW_SWAPPERS.length) fillAddressTable('sw_swappers_tbl', SW_SWAPPERS, SW_SWAPPERS[0].count);
    }}
    if (tab === 'bridge') {{
        lineChart('br_daily', BR_DAILY.map(d=>d.date), BR_DAILY.map(d=>d.count), '#39d2c0', 'events');
        doughnutChart('br_types', BR_TYPES.map(t=>t.name), BR_TYPES.map(t=>t.count));
        barChart('br_weekly', BR_WEEKLY.map(d=>d.date), BR_WEEKLY.map(d=>d.count), '#d29922', 'events', false);
        if(BR_CONTRACTS.length) fillEnrichedTable('br_contracts_tbl', BR_CONTRACTS, BR_CONTRACTS[0].count, {{showName:true}});
        if(BR_TOKENS.length) fillEnrichedTable('br_tokens_tbl', BR_TOKENS, BR_TOKENS[0].count, {{showName:true}});
        // Chain origins/destinations with names
        barChart('br_origins', BR_ORIGINS.map(o=>chainName(o.chain_id)), BR_ORIGINS.map(o=>o.count), '#f778ba', 'events', true);
        barChart('br_dests', BR_DESTS.map(o=>chainName(o.chain_id)), BR_DESTS.map(o=>o.count), '#58a6ff', 'events', true);
    }}
    if (tab === 'transfers') {{
        lineChart('tf_daily', TF_DAILY.map(d=>d.date), TF_DAILY.map(d=>d.count), '#3fb950', 'transfers');
        barChart('tf_weekly', TF_WEEKLY.map(d=>d.date), TF_WEEKLY.map(d=>d.count), '#bc8cff', 'transfers', false);
        if(TF_TOKENS.length) fillEnrichedTable('tf_tokens_tbl', TF_TOKENS, TF_TOKENS[0].count, {{showName:true}});
        if(TF_SENDERS.length) fillAddressTable('tf_senders_tbl', TF_SENDERS, TF_SENDERS[0].count);
        if(TF_RECEIVERS.length) fillAddressTable('tf_receivers_tbl', TF_RECEIVERS, TF_RECEIVERS[0].count);
    }}
    if (tab === 'contracts') {{
        barChart('ct_weekly', CT_WEEKLY.map(d=>d.date), CT_WEEKLY.map(d=>d.count), '#d29922', 'contracts', false);
        // Top contracts table
        const ct = document.getElementById('ct_top_tbl');
        const mx = CT_TOP.length ? CT_TOP[0].count : 1;
        CT_TOP.forEach((c,i) => {{
            const pct = (c.count/mx*100).toFixed(1);
            const name = enrichAddr(c.address);
            const tr = document.createElement('tr');
            tr.innerHTML = `<td style="color:#8b949e">${{i+1}}</td><td>${{name ? `<span class="token-name">${{name}}</span>` : '<span style="color:#8b949e">Unknown</span>'}}</td><td class="mono" style="font-size:12px">${{shortAddr(c.address)}}</td><td class="num">${{c.count.toLocaleString()}}</td><td class="num">${{(c.first_block||0).toLocaleString()}}</td><td class="num">${{(c.last_block||0).toLocaleString()}}</td><td><div class="bar-wrap"><div class="bar-fill" style="width:${{pct}}%"></div><span style="font-size:11px;color:#8b949e">${{pct}}%</span></div></td>`;
            ct.appendChild(tr);
        }});
        // Diverse contracts
        const dt = document.getElementById('ct_diverse_tbl');
        CT_DIVERSE.forEach((c,i) => {{
            const name = enrichAddr(c.address);
            const tr = document.createElement('tr');
            tr.innerHTML = `<td style="color:#8b949e">${{i+1}}</td><td>${{name ? `<span class="token-name">${{name}}</span>` : '<span style="color:#8b949e">Unknown</span>'}}</td><td class="mono" style="font-size:12px">${{shortAddr(c.address)}}</td><td class="num">${{c.event_types}}</td><td class="num">${{c.total.toLocaleString()}}</td>`;
            dt.appendChild(tr);
        }});
        // Contract events breakdown
        const et = document.getElementById('ct_events_tbl');
        CT_EVENTS.forEach(c => {{
            const name = enrichAddr(c.address);
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${{name ? `<span class="token-name">${{name}}</span> <span class="mono" style="font-size:11px;opacity:0.5">${{shortAddr(c.address)}}</span>` : `<span class="mono">${{shortAddr(c.address)}}</span>`}}</td><td>${{c.event}}</td><td class="num">${{c.count.toLocaleString()}}</td>`;
            et.appendChild(tr);
        }});
    }}
}}

// ═══════ INIT OVERVIEW ON LOAD ═══════
chartsInit['overview'] = true;
lineChart('ov_daily_txs', OV_TXS.map(d=>d.date), OV_TXS.map(d=>d.count), '#34d399', 'transactions');
doughnutChart('ov_types', OV_EVENT_TYPES.map(e=>e.name), OV_EVENT_TYPES.map(e=>e.count));
lineChart('ov_active_addr', OV_TXS.map(d=>d.date), OV_TXS.map(d=>d.accounts), '#c084fc', 'addresses');
lineChart('ov_gas_util', OV_BLOCKS.map(d=>d.date), OV_BLOCKS.map(d=>(d.utilization*100).toFixed(2)), '#fbbf24', '% utilized');
lineChart('ov_cum_txs', OV_TXS.map(d=>d.date), OV_TXS.map(d=>d.cum_txs), '#60a5fa', 'cumulative txs');
lineChart('ov_cum_contracts', OV_CONTRACTS.map(d=>d.date), OV_CONTRACTS.map(d=>d.cum_contracts), '#f472b6', 'contracts');
</script>
</body>
</html>"""


# ── MAIN ─────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("INCENTIV DASHBOARD GENERATOR (5 tabs + enrichment)")
    print("=" * 60)

    print("\nLoading enrichment data...")
    enrichment = load_enrichment()
    print(f"  Loaded {len(enrichment)} enriched contracts")

    neon = NeonLoader()
    print("\nQuerying Neon DB...\n")

    overview = query_overview(neon)
    swaps = query_swaps(neon)
    bridge = query_bridge(neon)
    transfers = query_transfers(neon)
    contracts_data = query_contracts(neon)
    neon.close()

    print("\nGenerating dashboard HTML...")
    html = generate_html(overview, swaps, bridge, transfers, contracts_data, enrichment)

    out_dir = Path("dashboard")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "incentiv_dashboard.html"
    out_path.write_text(html, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    print(f"\nDashboard saved: {out_path.resolve()}")
    print(f"  Size: {size_kb:.0f} KB")

    try:
        webbrowser.open(f"file://{out_path.resolve()}")
        print("  Opened in browser!")
    except Exception:
        print("  Open the file manually in your browser.")

    print("\nRe-run anytime to refresh with latest data.")


if __name__ == "__main__":
    main()
