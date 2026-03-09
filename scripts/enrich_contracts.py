#!/usr/bin/env python3
"""
Enriches contract data using the Blockscout REST API.
- Resolves contract addresses to names, token symbols, types
- Inspects decoded event params to find correct field names
- Saves enrichment data to dashboard/enrichment.json

Usage: python3 scripts/enrich_contracts.py
"""
import json
import sys
import time
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader

EXPLORER_API = "https://explorer.incentiv.io/api/v2"
RATE_LIMIT = 0.2  # seconds between API calls


def fetch_token_info(address):
    """Fetch token info from Blockscout API."""
    try:
        resp = requests.get(f"{EXPLORER_API}/tokens/{address}", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "name": data.get("name"),
                "symbol": data.get("symbol"),
                "decimals": data.get("decimals"),
                "type": data.get("type"),
                "total_supply": data.get("total_supply"),
                "holders_count": data.get("holders_count"),
            }
    except Exception as e:
        pass
    return None


def fetch_address_info(address):
    """Fetch address info from Blockscout API."""
    try:
        resp = requests.get(f"{EXPLORER_API}/addresses/{address}", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "name": data.get("name"),
                "is_contract": data.get("is_contract"),
                "is_verified": data.get("is_verified"),
                "implementation_name": data.get("implementation_name"),
            }
    except Exception:
        pass
    return None


def fetch_smart_contract(address):
    """Fetch smart contract details from Blockscout API."""
    try:
        resp = requests.get(f"{EXPLORER_API}/smart-contracts/{address}", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "name": data.get("name"),
                "compiler_version": data.get("compiler_version"),
                "is_verified": data.get("is_verified"),
            }
    except Exception:
        pass
    return None


def inspect_event_params(neon, event_name, limit=20):
    """Inspect params for a given event type to find field names."""
    rows = neon.query(f"""
        SELECT contract_address, params
        FROM decoded_events
        WHERE event_name = '{event_name}' AND params IS NOT NULL
        LIMIT {limit}
    """)
    all_keys = {}
    samples = []
    for addr, p in rows:
        params = p if isinstance(p, dict) else json.loads(p) if p else {}
        for k in params.keys():
            all_keys[k] = all_keys.get(k, 0) + 1
        if len(samples) < 5:
            samples.append({"contract": addr, "params": params})
    return {"field_counts": all_keys, "samples": samples}


def main():
    print("=" * 60)
    print("CONTRACT & EVENT ENRICHMENT")
    print("=" * 60)

    neon = NeonLoader()

    # ── 1. Inspect event params for each major event type ──
    print("\n[1/3] Inspecting decoded event params...\n")
    event_types = ["Swap", "Transfer", "Approval", "SentTransferRemote",
                   "ReceivedTransferRemote", "Dispatch", "Process",
                   "Mint", "Burn", "Collect"]
    param_info = {}
    for ev in event_types:
        info = inspect_event_params(neon, ev)
        if info["field_counts"]:
            param_info[ev] = info
            print(f"  {ev}:")
            print(f"    Fields: {json.dumps(info['field_counts'], indent=None)}")
            if info["samples"]:
                print(f"    Sample: {json.dumps(info['samples'][0]['params'], indent=None)[:200]}")
            print()

    # ── 2. Get all contract addresses from Neon ──
    print("[2/3] Fetching contract addresses from Neon...")
    rows = neon.query("""
        SELECT address, event_count FROM contracts
        ORDER BY event_count DESC LIMIT 50
    """)
    contract_addresses = [(r[0], r[1]) for r in rows]
    print(f"  Found {len(contract_addresses)} contracts to enrich.\n")

    # Also get top swap pool addresses
    rows = neon.query("""
        SELECT DISTINCT contract_address FROM decoded_events
        WHERE event_name = 'Swap' LIMIT 20
    """)
    swap_pools = [r[0] for r in rows]

    # And top transfer token addresses
    rows = neon.query("""
        SELECT contract_address, COUNT(*) as cnt FROM decoded_events
        WHERE event_name = 'Transfer'
        GROUP BY contract_address ORDER BY cnt DESC LIMIT 20
    """)
    transfer_tokens = [(r[0], r[1]) for r in rows]

    neon.close()

    # ── 3. Enrich via Blockscout API ──
    print("[3/3] Enriching via Blockscout API...\n")

    enriched = {}
    all_addresses = set()
    for addr, _ in contract_addresses:
        all_addresses.add(addr)
    for addr in swap_pools:
        all_addresses.add(addr)
    for addr, _ in transfer_tokens:
        all_addresses.add(addr)

    total = len(all_addresses)
    for i, addr in enumerate(sorted(all_addresses)):
        print(f"  [{i+1}/{total}] {addr[:16]}...", end=" ")

        # Try token info first
        token = fetch_token_info(addr)
        time.sleep(RATE_LIMIT)

        # Then address info
        address_info = fetch_address_info(addr)
        time.sleep(RATE_LIMIT)

        # Then smart contract info
        sc_info = fetch_smart_contract(addr)
        time.sleep(RATE_LIMIT)

        entry = {"address": addr}
        if token:
            entry["token_name"] = token["name"]
            entry["token_symbol"] = token["symbol"]
            entry["token_decimals"] = token["decimals"]
            entry["token_type"] = token["type"]
            entry["holders_count"] = token["holders_count"]
            print(f"TOKEN: {token['name']} ({token['symbol']})")
        elif sc_info and sc_info.get("name"):
            entry["contract_name"] = sc_info["name"]
            entry["is_verified"] = sc_info.get("is_verified", False)
            print(f"CONTRACT: {sc_info['name']}")
        elif address_info and address_info.get("name"):
            entry["address_name"] = address_info["name"]
            entry["is_contract"] = address_info.get("is_contract", False)
            print(f"ADDRESS: {address_info['name']}")
        else:
            entry["unknown"] = True
            print("(unknown)")

        enriched[addr] = entry

    # ── Save results ──
    out_dir = Path("dashboard")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "enrichment.json"

    result = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M UTC"),
        "param_info": param_info,
        "contracts": enriched,
        "swap_pools": swap_pools,
        "transfer_tokens": [{"address": a, "count": c} for a, c in transfer_tokens],
    }

    out_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(f"\nEnrichment saved to: {out_path.resolve()}")
    print(f"  Contracts enriched: {len(enriched)}")
    print(f"  Event types inspected: {len(param_info)}")
    print("\nNow re-run: python3 scripts/generate_dashboard.py")


if __name__ == "__main__":
    main()
