#!/usr/bin/env python3
"""
Audit script to check current data state and identify gaps in blockchain coverage.
"""
import json
import os
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuration
BLOCKSCOUT_RPC = os.getenv("INCENTIV_BLOCKSCOUT_RPC_URL")
DATA_DIR = Path(__file__).parent.parent / "incentiv_data"
STATE_FILE = Path(__file__).parent.parent / "state.json"
BACKFILL_STATE_FILE = Path(__file__).parent.parent / "state_backfill.json"

def get_current_block_height():
    """Get the current block height from the blockchain."""
    try:
        response = requests.post(
            BLOCKSCOUT_RPC,
            json={
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            },
            timeout=10
        )
        result = response.json()
        return int(result["result"], 16)
    except Exception as e:
        print(f"❌ Error fetching current block: {e}")
        return None

def load_state_files():
    """Load state files to check last processed blocks."""
    states = {}
    
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            states["main"] = json.load(f)
    
    if BACKFILL_STATE_FILE.exists():
        with open(BACKFILL_STATE_FILE) as f:
            states["backfill"] = json.load(f)
    
    return states

def analyze_csv_files():
    """Analyze CSV files to get record counts and date ranges."""
    csv_files = list(DATA_DIR.glob("*.csv"))
    analysis = {}
    
    for csv_file in csv_files:
        with open(csv_file) as f:
            lines = f.readlines()
            record_count = len(lines) - 1  # Exclude header
            
            analysis[csv_file.name] = {
                "records": record_count,
                "size_mb": csv_file.stat().st_size / (1024 * 1024),
                "modified": datetime.fromtimestamp(csv_file.stat().st_mtime).isoformat()
            }
    
    return analysis

def main():
    print("=" * 80)
    print("INCENTIV DATA STATE AUDIT")
    print("=" * 80)
    print()
    
    # 1. Current blockchain height
    print("📊 BLOCKCHAIN STATUS")
    print("-" * 80)
    current_block = get_current_block_height()
    if current_block:
        print(f"✅ Current blockchain height: {current_block:,}")
    else:
        print("❌ Could not fetch current blockchain height")
    print()
    
    # 2. State files
    print("📁 STATE FILES")
    print("-" * 80)
    states = load_state_files()
    
    if "main" in states:
        print(f"Main ETL State:")
        print(f"  Last processed block: {states['main'].get('last_block', 'N/A'):,}")
        print(f"  Last chain block: {states['main'].get('last_chain_block', 'N/A'):,}")
    
    if "backfill" in states:
        print(f"Backfill State:")
        print(f"  Last backfill block: {states['backfill'].get('last_block', 'N/A'):,}")
    print()
    
    # 3. Gap analysis
    if current_block and "main" in states and "backfill" in states:
        print("🔍 GAP ANALYSIS")
        print("-" * 80)
        
        main_last = states["main"].get("last_block", 0)
        backfill_last = states["backfill"].get("last_block", 0)
        
        # Gap between backfill and main ETL
        if backfill_last < main_last:
            gap_backfill_to_main = main_last - backfill_last
            print(f"⚠️  Gap between backfill and main ETL: {gap_backfill_to_main:,} blocks")
            print(f"   Backfill ended at: {backfill_last:,}")
            print(f"   Main ETL started at: {main_last:,}")
        else:
            print(f"✅ Backfill ({backfill_last:,}) has reached/passed main ETL ({main_last:,})")
        
        # Gap between main ETL and current blockchain
        gap_main_to_current = current_block - main_last
        print(f"📈 Blocks since last main ETL run: {gap_main_to_current:,}")
        print(f"   Last processed: {main_last:,}")
        print(f"   Current height: {current_block:,}")
        
        # Coverage percentage
        coverage = (backfill_last / current_block) * 100 if current_block > 0 else 0
        print(f"📊 Historical coverage: {coverage:.2f}% (block {backfill_last:,} of {current_block:,})")
        print()
    
    # 4. CSV file analysis
    print("📄 CSV DATA FILES")
    print("-" * 80)
    csv_analysis = analyze_csv_files()
    
    total_records = 0
    for filename, info in sorted(csv_analysis.items()):
        print(f"{filename}:")
        print(f"  Records: {info['records']:,}")
        print(f"  Size: {info['size_mb']:.2f} MB")
        print(f"  Last modified: {info['modified']}")
        total_records += info['records']
    
    print(f"\nTotal records across all files: {total_records:,}")
    print()
    
    # 5. Recommendations
    print("💡 RECOMMENDATIONS")
    print("-" * 80)
    
    if current_block and "main" in states:
        main_last = states["main"].get("last_block", 0)
        gap = current_block - main_last
        
        if gap > 1000:
            print(f"⚠️  Large gap detected ({gap:,} blocks)")
            print("   → Run continuous ETL to catch up to current block")
        elif gap > 100:
            print(f"⚠️  Moderate gap detected ({gap:,} blocks)")
            print("   → Consider running ETL update")
        else:
            print(f"✅ Data is relatively up-to-date (gap: {gap:,} blocks)")
    
    if "backfill" in states and current_block:
        backfill_last = states["backfill"].get("last_block", 0)
        if backfill_last < current_block:
            remaining = current_block - backfill_last
            print(f"📋 Backfill remaining: {remaining:,} blocks")
            print(f"   → Continue backfill from block {backfill_last:,} to {current_block:,}")
    
    print()
    print("=" * 80)

if __name__ == "__main__":
    main()
