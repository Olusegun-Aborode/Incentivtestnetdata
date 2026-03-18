#!/usr/bin/env python3
"""
BLOCKSCOUT API v2 DATA SUPPLEMENT
Fills gaps in our Neon DB using Blockscout's pre-indexed data.

The Blockscout explorer has already indexed the ENTIRE Incentiv chain.
Instead of re-crawling 1.46M missing blocks via slow RPC calls,
we pull the data directly from Blockscout's REST API.

Strategy:
  Phase 1: Stats & Metadata (fast, one-shot)
    - Chain stats → dashboard KPIs
    - All tokens → enrichment
    - All verified contracts → contracts table

  Phase 2: Transactions & Logs for missing blocks (paginated bulk)
    - For each missing block range, fetch transactions via API
    - Extract logs from transaction details
    - Insert into our Neon DB

Usage:
  python3 scripts/blockscout_supplement.py --stats          # Phase 1: stats only
  python3 scripts/blockscout_supplement.py --contracts      # Phase 1: all contracts
  python3 scripts/blockscout_supplement.py --tokens         # Phase 1: all tokens
  python3 scripts/blockscout_supplement.py --backfill       # Phase 2: missing block data
  python3 scripts/blockscout_supplement.py --all            # Everything
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader

BASE_URL = "https://explorer.incentiv.io/api/v2"
RATE_LIMIT_DELAY = 0.25  # seconds between API calls


# ── HELPERS ──────────────────────────────────────────────────────
def api_get(endpoint: str, params: dict = None, retries: int = 3) -> Optional[dict]:
    """GET from Blockscout API with retry logic."""
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  API error {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            print(f"  Request error (attempt {attempt+1}): {e}")
            time.sleep(1)
    return None


def paginate_all(endpoint: str, max_pages: int = 0, items_key: str = "items") -> List[dict]:
    """Fetch ALL pages from a paginated Blockscout endpoint."""
    all_items = []
    params = {}
    page = 0

    while True:
        page += 1
        data = api_get(endpoint, params=params)
        if not data:
            break

        items = data.get(items_key, [])
        if not items:
            break

        all_items.extend(items)
        print(f"    Page {page}: +{len(items)} items (total: {len(all_items)})")

        # Check for next page
        next_page = data.get("next_page_params")
        if not next_page:
            break

        params = next_page

        if max_pages and page >= max_pages:
            print(f"    Reached max_pages={max_pages}, stopping.")
            break

        time.sleep(RATE_LIMIT_DELAY)

    return all_items


# ══════════════════════════════════════════════════════════════════
# PHASE 1: STATS & METADATA
# ══════════════════════════════════════════════════════════════════

def fetch_chain_stats():
    """Fetch and display live chain stats from Blockscout."""
    print("\n" + "="*60)
    print("  BLOCKSCOUT CHAIN STATS")
    print("="*60)

    data = api_get("stats")
    if not data:
        print("  Failed to fetch stats!")
        return None

    # Save raw stats
    stats_path = Path("dashboard/blockscout_stats.json")
    stats_path.parent.mkdir(exist_ok=True)
    stats_path.write_text(json.dumps(data, indent=2))
    print(f"  Saved raw stats to {stats_path}")

    # Display key metrics
    print(f"\n  {'Metric':<30s} {'Value':>15s}")
    print(f"  {'-'*45}")
    fields = [
        ("total_blocks", "Total Blocks"),
        ("total_transactions", "Total Transactions"),
        ("total_addresses", "Total Addresses"),
        ("average_block_time", "Avg Block Time (ms)"),
        ("coin_price", "CENT Price"),
        ("total_gas_used", "Total Gas Used"),
        ("network_utilization_percentage", "Network Utilization %"),
        ("market_cap", "Market Cap"),
        ("gas_prices", "Gas Prices"),
        ("gas_price_updated_at", "Gas Updated At"),
    ]
    for key, label in fields:
        val = data.get(key, "N/A")
        if isinstance(val, dict):
            val = json.dumps(val)
        print(f"  {label:<30s} {str(val):>15s}")

    return data


def fetch_all_tokens(neon: NeonLoader):
    """Fetch all tokens from Blockscout and update enrichment."""
    print("\n" + "="*60)
    print("  FETCHING ALL TOKENS FROM BLOCKSCOUT")
    print("="*60)

    tokens = paginate_all("tokens", max_pages=20)
    print(f"\n  Total tokens fetched: {len(tokens)}")

    # Update enrichment.json
    enrich_path = Path("dashboard/enrichment.json")
    if enrich_path.exists():
        enrichment = json.loads(enrich_path.read_text())
    else:
        enrichment = {"contracts": {}, "swap_pools": [], "transfer_tokens": []}

    contracts = enrichment.get("contracts", {})
    new_count = 0

    for token in tokens:
        addr = (token.get("address") or "").lower()
        if not addr:
            continue

        existing = contracts.get(addr, {})
        contracts[addr] = {
            **existing,
            "token_name": token.get("name", existing.get("token_name", "")),
            "token_symbol": token.get("symbol", existing.get("token_symbol", "")),
            "token_type": token.get("type", existing.get("token_type", "")),
            "decimals": token.get("decimals", existing.get("decimals")),
            "total_supply": token.get("total_supply", existing.get("total_supply")),
            "holders_count": token.get("holders_count", existing.get("holders_count")),
            "exchange_rate": token.get("exchange_rate", existing.get("exchange_rate")),
            "icon_url": token.get("icon_url", existing.get("icon_url")),
            "source": "blockscout_api",
        }
        if addr not in existing:
            new_count += 1

    enrichment["contracts"] = contracts
    enrichment["blockscout_tokens"] = tokens  # raw data for reference
    enrich_path.write_text(json.dumps(enrichment, indent=2, default=str))
    print(f"  Updated enrichment.json: {new_count} new tokens, {len(contracts)} total contracts")

    return tokens


def fetch_all_contracts(neon: NeonLoader):
    """Fetch all verified smart contracts from Blockscout."""
    print("\n" + "="*60)
    print("  FETCHING SMART CONTRACTS FROM BLOCKSCOUT")
    print("="*60)

    # Blockscout /smart-contracts only returns VERIFIED contracts
    # For all contract addresses, we need to use the addresses endpoint
    # But let's start with verified contracts for ABI data
    contracts = paginate_all("smart-contracts", max_pages=100)
    print(f"\n  Total verified contracts fetched: {len(contracts)}")

    if not contracts:
        return []

    # Update enrichment with contract names
    enrich_path = Path("dashboard/enrichment.json")
    if enrich_path.exists():
        enrichment = json.loads(enrich_path.read_text())
    else:
        enrichment = {"contracts": {}}

    enrich_contracts = enrichment.get("contracts", {})
    abi_dir = Path("config/abis/blockscout")
    abi_dir.mkdir(parents=True, exist_ok=True)

    for c in contracts:
        addr_obj = c.get("address", {})
        addr = (addr_obj.get("hash") if isinstance(addr_obj, dict) else str(addr_obj) or "").lower()
        if not addr:
            continue

        existing = enrich_contracts.get(addr, {})
        enrich_contracts[addr] = {
            **existing,
            "contract_name": c.get("name", existing.get("contract_name", "")),
            "is_verified": True,
            "compiler_version": c.get("compiler_version", ""),
            "optimization": c.get("optimization_enabled", False),
            "source": "blockscout_api",
        }

        # Save ABI if available
        abi = c.get("abi")
        if abi:
            safe_name = c.get("name", addr[:10]).replace("/", "_").replace(" ", "_")
            abi_path = abi_dir / f"{safe_name}_{addr[:10]}.json"
            abi_path.write_text(json.dumps(abi, indent=2))

    enrichment["contracts"] = enrich_contracts
    enrich_path.write_text(json.dumps(enrichment, indent=2, default=str))
    print(f"  Updated enrichment.json with {len(contracts)} verified contracts")
    print(f"  ABIs saved to {abi_dir}/")

    # Also upsert into our contracts table
    inserted = 0
    for c in contracts:
        addr_obj = c.get("address", {})
        addr = (addr_obj.get("hash") if isinstance(addr_obj, dict) else str(addr_obj) or "").lower()
        if not addr:
            continue
        try:
            cur = neon.conn.cursor()
            cur.execute(f"""
                INSERT INTO contracts (address, contract_type, is_decoded)
                VALUES ('{addr}', 'verified', TRUE)
                ON CONFLICT (address) DO UPDATE SET
                    is_decoded = TRUE,
                    contract_type = COALESCE(contracts.contract_type, 'verified'),
                    updated_at = NOW()
            """)
            inserted += 1
        except Exception:
            pass
    try:
        neon.conn.commit()
    except Exception:
        pass
    print(f"  Upserted {inserted} contracts into Neon contracts table")

    return contracts


# ══════════════════════════════════════════════════════════════════
# PHASE 2: MISSING BLOCK DATA VIA BLOCKSCOUT API
# ══════════════════════════════════════════════════════════════════

def find_missing_blocks(neon: NeonLoader) -> List[tuple]:
    """Identify ranges of missing blocks in our DB."""
    print("\n" + "="*60)
    print("  FINDING MISSING BLOCK RANGES")
    print("="*60)

    # Get our current block range
    rows = neon.query("SELECT MIN(number), MAX(number) FROM blocks")
    min_blk, max_blk = rows[0]
    print(f"  Our range: {min_blk:,} to {max_blk:,}")

    # Find gaps using window function
    print("  Scanning for gaps (this may take a moment)...")
    rows = neon.query("""
        SELECT gap_start, gap_end, gap_end - gap_start + 1 as gap_size
        FROM (
            SELECT
                number + 1 as gap_start,
                LEAD(number) OVER (ORDER BY number) - 1 as gap_end
            FROM blocks
        ) gaps
        WHERE gap_end IS NOT NULL AND gap_end >= gap_start
        ORDER BY gap_size DESC
        LIMIT 50
    """)

    gaps = [(int(r[0]), int(r[1]), int(r[2])) for r in rows]
    total_missing = sum(g[2] for g in gaps)

    print(f"\n  Found {len(gaps)} gap ranges, {total_missing:,} total missing blocks")
    print(f"\n  {'Start':>12s} {'End':>12s} {'Size':>10s}")
    print(f"  {'-'*34}")
    for start, end, size in gaps[:20]:
        print(f"  {start:>12,} {end:>12,} {size:>10,}")
    if len(gaps) > 20:
        print(f"  ... and {len(gaps)-20} more ranges")

    # Also check blocks before our earliest
    print(f"\n  Blocks 0 to {min_blk-1:,} = {min_blk:,} pre-genesis blocks NOT indexed")

    # Save gap data for the backfill script
    gap_path = Path("data/missing_blocks.json")
    gap_path.parent.mkdir(exist_ok=True)
    gap_data = {
        "generated_at": datetime.utcnow().isoformat(),
        "our_min": min_blk,
        "our_max": max_blk,
        "total_gaps": len(gaps),
        "total_missing_in_range": total_missing,
        "total_missing_before_range": min_blk,
        "gaps": [{"start": g[0], "end": g[1], "size": g[2]} for g in gaps],
    }
    gap_path.write_text(json.dumps(gap_data, indent=2))
    print(f"\n  Gap data saved to {gap_path}")

    return gaps


def backfill_missing_blocks(neon: NeonLoader, batch_size: int = 20, max_blocks: int = 0):
    """
    Backfill missing blocks using Blockscout API instead of RPC.

    For each missing block:
      1. GET /api/v2/blocks/{number} → block data
      2. GET /api/v2/blocks/{number}/transactions → all txs in that block

    FIXES applied (v2):
      - Uses parameterized queries (%s) instead of f-string SQL (prevents SQL injection
        and "no results to fetch" errors from broken SQL on edge-case data)
      - Proper cursor lifecycle (open, execute, close)
      - Explicit rollback on ANY failure before continuing
      - Handles None/missing timestamp gracefully
    """
    print("\n" + "="*60)
    print("  BACKFILLING MISSING BLOCKS VIA BLOCKSCOUT API")
    print("="*60)

    # Load gap data
    gap_path = Path("data/missing_blocks.json")
    if not gap_path.exists():
        print("  No gap data found. Run --find-gaps first.")
        return

    gap_data = json.loads(gap_path.read_text())
    gaps = gap_data["gaps"]

    total_to_fill = sum(g["size"] for g in gaps)
    print(f"  Total blocks to backfill: {total_to_fill:,}")
    if max_blocks:
        print(f"  Max blocks this run: {max_blocks:,}")

    filled = 0
    errors = 0
    skipped = 0
    start_time = time.time()

    # SQL with %s parameterized placeholders — NEVER use f-strings for values
    BLOCK_INSERT_SQL = """
        INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used,
                            gas_limit, base_fee_per_gas, miner, size,
                            transaction_count, nonce, chain, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'incentiv', NOW())
        ON CONFLICT (number) DO NOTHING
    """

    TX_INSERT_SQL = """
        INSERT INTO transactions (hash, block_number, from_address, to_address,
                                   value, gas_price, gas, gas_used, status,
                                   nonce, transaction_index, block_hash,
                                   block_timestamp, chain, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'incentiv', NOW())
        ON CONFLICT (hash) DO NOTHING
    """

    for gap in gaps:
        gap_start = gap["start"]
        gap_end = gap["end"]

        for block_num in range(gap_start, gap_end + 1):
            if max_blocks and filled >= max_blocks:
                print(f"\n  Reached max_blocks={max_blocks}, stopping.")
                break

            # Fetch block from Blockscout
            block_data = api_get(f"blocks/{block_num}")
            if not block_data:
                errors += 1
                continue

            # Parse block fields safely
            try:
                b_number = int(block_data.get("height", block_num))
                b_hash = block_data.get("hash", "")
                b_parent = block_data.get("parent_hash", "")
                b_timestamp = block_data.get("timestamp") or None  # None if empty/missing
                b_gas_used = int(block_data.get("gas_used", "0") or "0")
                b_gas_limit = int(block_data.get("gas_limit", "0") or "0")
                b_base_fee = int(block_data.get("base_fee_per_gas", "0") or "0")
                miner_obj = block_data.get("miner") or {}
                b_miner = miner_obj.get("hash", "") if isinstance(miner_obj, dict) else str(miner_obj)
                b_size = int(block_data.get("size", 0) or 0)
                b_tx_count = int(block_data.get("tx_count", 0) or 0)
                b_nonce = block_data.get("nonce", "") or ""
            except (ValueError, TypeError) as e:
                print(f"  Block {block_num} parse error: {e}")
                errors += 1
                continue

            # Insert block with parameterized query
            try:
                cur = neon.conn.cursor()
                cur.execute(BLOCK_INSERT_SQL, (
                    b_number, b_hash, b_parent, b_timestamp,
                    b_gas_used, b_gas_limit, b_base_fee, b_miner,
                    b_size, b_tx_count, b_nonce
                ))
                cur.close()
            except Exception as e:
                print(f"  Block {block_num} insert error: {e}")
                try:
                    neon.conn.rollback()
                except Exception:
                    pass
                errors += 1
                continue

            # Fetch and insert transactions for this block
            if b_tx_count > 0:
                txs = paginate_all(f"blocks/{block_num}/transactions", max_pages=10)
                for tx in txs:
                    try:
                        tx_hash = tx.get("hash", "")
                        if not tx_hash:
                            continue

                        # Blockscout wraps from/to in objects: {"hash": "0x...", "name": "..."}
                        from_obj = tx.get("from") or {}
                        to_obj = tx.get("to") or {}
                        from_addr = (from_obj.get("hash", "") if isinstance(from_obj, dict) else str(from_obj)).lower()
                        to_addr = (to_obj.get("hash", "") if isinstance(to_obj, dict) else str(to_obj)).lower()

                        cur = neon.conn.cursor()
                        cur.execute(TX_INSERT_SQL, (
                            tx_hash,
                            b_number,
                            from_addr or None,
                            to_addr or None,
                            str(tx.get("value", "0")),
                            str(tx.get("gas_price", "0")),
                            int(tx.get("gas_limit", 0) or 0),
                            int(tx.get("gas_used", 0) or 0),
                            str(tx.get("status", "")),
                            str(tx.get("nonce", "0")),
                            int(tx.get("position", 0) or 0),
                            b_hash,
                            b_timestamp,
                        ))
                        cur.close()
                    except Exception as e:
                        # Don't let one bad tx kill the whole block
                        try:
                            neon.conn.rollback()
                        except Exception:
                            pass
                        continue

            # Commit the entire block + its transactions
            try:
                neon.conn.commit()
            except Exception as e:
                print(f"  Block {block_num} commit error: {e}")
                try:
                    neon.conn.rollback()
                except Exception:
                    pass
                errors += 1
                continue

            filled += 1

            if filled % batch_size == 0:
                elapsed = time.time() - start_time
                rate = filled / elapsed if elapsed > 0 else 0
                eta_s = (total_to_fill - filled) / rate if rate > 0 else 0
                eta_h = eta_s / 3600
                print(f"  [{filled:,}/{total_to_fill:,}] {filled/total_to_fill*100:.1f}% | "
                      f"{rate:.1f} blocks/s | ETA: {eta_h:.1f}h | Errors: {errors}")

            time.sleep(RATE_LIMIT_DELAY)

        if max_blocks and filled >= max_blocks:
            break

    # Final commit just in case
    try:
        neon.conn.commit()
    except Exception:
        pass

    elapsed = time.time() - start_time
    print(f"\n  DONE: Filled {filled:,} blocks in {elapsed:.0f}s ({errors} errors)")


# ══════════════════════════════════════════════════════════════════
# PHASE 3: SUPPLEMENT DASHBOARD WITH BLOCKSCOUT STATS
# ══════════════════════════════════════════════════════════════════

def update_dashboard_stats():
    """
    Save Blockscout stats for the dashboard to use as KPI overlay.
    The dashboard can show Blockscout totals as "Network" KPIs
    and our indexed data as "Indexed" detail charts.
    """
    print("\n" + "="*60)
    print("  UPDATING DASHBOARD WITH BLOCKSCOUT STATS")
    print("="*60)

    data = api_get("stats")
    if not data:
        print("  Failed to fetch stats!")
        return

    # Parse into dashboard-friendly format
    dashboard_stats = {
        "source": "blockscout_api",
        "fetched_at": datetime.utcnow().isoformat(),
        "total_blocks": int(data.get("total_blocks", "0").replace(",", "")),
        "total_transactions": int(data.get("total_transactions", "0").replace(",", "")),
        "total_addresses": int(data.get("total_addresses", "0").replace(",", "")),
        "average_block_time_ms": float(data.get("average_block_time", "0")),
        "coin_price": data.get("coin_price"),
        "market_cap": data.get("market_cap"),
        "network_utilization": float(data.get("network_utilization_percentage", "0")),
        "gas_prices": data.get("gas_prices", {}),
        "gas_prices_updated_at": data.get("gas_price_updated_at"),
        # Additional stats from counters
        "total_gas_used": data.get("total_gas_used"),
        "static_gas_price": data.get("static_gas_price"),
    }

    stats_path = Path("dashboard/blockscout_stats.json")
    stats_path.write_text(json.dumps(dashboard_stats, indent=2, default=str))
    print(f"  Saved dashboard stats to {stats_path}")
    print(f"  Total blocks:       {dashboard_stats['total_blocks']:,}")
    print(f"  Total transactions: {dashboard_stats['total_transactions']:,}")
    print(f"  Total addresses:    {dashboard_stats['total_addresses']:,}")

    return dashboard_stats


# ── MAIN ─────────────────────────────────────────────────────────
def parse_arguments():
    parser = argparse.ArgumentParser(description="Blockscout API Data Supplement")
    parser.add_argument("--stats", action="store_true", help="Fetch chain stats")
    parser.add_argument("--tokens", action="store_true", help="Fetch all tokens")
    parser.add_argument("--contracts", action="store_true", help="Fetch verified contracts")
    parser.add_argument("--find-gaps", action="store_true", help="Find missing block ranges")
    parser.add_argument("--backfill", action="store_true", help="Backfill missing blocks via API")
    parser.add_argument("--max-blocks", type=int, default=0, help="Max blocks to backfill")
    parser.add_argument("--batch-size", type=int, default=20, help="Progress report interval")
    parser.add_argument("--all", action="store_true", help="Run everything")
    return parser.parse_args()


def main():
    args = parse_arguments()

    if not any([args.stats, args.tokens, args.contracts, args.find_gaps, args.backfill, args.all]):
        print("Usage: python3 scripts/blockscout_supplement.py --stats|--tokens|--contracts|--find-gaps|--backfill|--all")
        print("\nRecommended order:")
        print("  1. --stats         Get live chain stats")
        print("  2. --tokens        Fetch all token metadata")
        print("  3. --contracts     Fetch verified contract ABIs")
        print("  4. --find-gaps     Identify missing block ranges")
        print("  5. --backfill      Fill missing blocks via Blockscout API")
        return

    neon = NeonLoader()

    print("="*60)
    print("BLOCKSCOUT API v2 DATA SUPPLEMENT")
    print("="*60)

    if args.stats or args.all:
        fetch_chain_stats()
        update_dashboard_stats()

    if args.tokens or args.all:
        fetch_all_tokens(neon)

    if args.contracts or args.all:
        fetch_all_contracts(neon)

    if args.find_gaps or args.all:
        find_missing_blocks(neon)

    if args.backfill:
        backfill_missing_blocks(neon, batch_size=args.batch_size, max_blocks=args.max_blocks)

    neon.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
