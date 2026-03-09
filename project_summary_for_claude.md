# Walkthrough: Data Synchronization Recovery

## Overview
We successfully restored the data synchronization pipeline and accelerated the catch-up process to close a significant block lag.

## Actions Taken

### 1. RPC URL Fix
- **Issue:** The continuous sync script (`run_continuous.py`) was failing because it couldn't connect to the blockchain.
- **Root Cause:** Missing `INCENTIV_BLOCKSCOUT_RPC_URL` environment variable.
- **Fix:** Identified the correct RPC URL (`https://explorer.incentiv.io/api/eth-rpc`) and added it to `.env`.

### 2. Parallel Backfill Acceleration
- **Issue:** A backlog of ~320,000 blocks existed (Live Chain: ~2.29M, Database: ~1.97M).
- **Strategy:** Implemented a parallel backfill approach to clear the backlog rapidly.
- **Implementation:**
    - Created `scripts/parallel_backfill.sh` to run 4 concurrent jobs targeting different block ranges.
    - Created `scripts/continuous_upload.sh` to upload extracted data immediately.
- **Result:** Reduced lag from ~320k blocks to ~54k blocks in minutes.

### 3. Continuous Sync Restoration
- **Action:** Transitioned from parallel backfill back to the single-threaded `run_continuous.py` process.
- **Status:** The script is now running stably and efficiently processing the remaining ~50k blocks.

## Verification
- **Sync Status Check:** `python3 scripts/check_sync_status.py`
    - Confirmed Supabase max block is increasing (currently ~2.24M).
    - Confirmed lag is decreasing.

## Next Steps
- Allow the continuous sync process to finish closing the small remaining gap.
- Proceed with Dashboard deployment testing once data is fully current.


# Key Scripts Source Code


## scripts/run_continuous.py
```python
#!/usr/bin/env python3
"""
Continuous ETL sync for Incentiv blockchain.
Runs continuously to keep data up-to-date with the latest blocks.
"""
import subprocess
import time
import sys
import json
from datetime import datetime
from pathlib import Path

STATE_FILE = Path(__file__).parent.parent / "state.json"
LOG_FILE = Path(__file__).parent.parent / "continuous_sync.log"

def log(message):
    """Log message to both console and file."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    with open(LOG_FILE, 'a') as f:
        f.write(log_msg + '\n')

def get_last_processed_block():
    """Get the last processed block from state file."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            state = json.load(f)
            return state.get('last_block', 0)
    return 0

def main():
    log("🚀 Starting Continuous Sync for Incentiv ETL")
    log("This will run indefinitely. Press Ctrl+C to stop.")
    log("=" * 80)
    
    # Configuration
    BATCH_SIZE = 2000  # Increased for faster catch-up
    SLEEP_TIME = 60    # Sleep time between runs (seconds)
    ERROR_THRESHOLD = 10 # Increase tolerance
    consecutive_errors = 0
    # Configuration
    BATCH_SIZE = 2000  # Increased for faster catch-up
    SLEEP_SECONDS = 30 # Reduced sleep for faster catch-up
    ERROR_THRESHOLD = 10 
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5
    
    while True:
        try:
            start_time = datetime.now()
            last_block = get_last_processed_block()
            
            log(f"Starting sync run (last processed block: {last_block:,})")
            
            # Run the pipeline module with batch size
            result = subprocess.run(
                [
                    sys.executable, "-u", "-m", "src.pipeline",
                    "--chain", "incentiv",
                    "--logs",
                    "--decoded-logs",
                    "--skip-dune",
                    "--batch-size", str(BATCH_SIZE)
                ],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                new_block = get_last_processed_block()
                blocks_processed = new_block - last_block
                
                if blocks_processed > 0:
                    log(f"✅ Processed {blocks_processed} new blocks (now at block {new_block:,})")
                    
                    # Trigger upload to Supabase
                    log("Uploading recent data to Supabase...")
                    upload_res = subprocess.run([sys.executable, "scripts/upload_recent.py"], capture_output=True, text=True)
                    if upload_res.returncode == 0:
                        log(f"Upload success: {upload_res.stdout.strip()}")
                    else:
                        log(f"Upload failed: {upload_res.stderr}")

                else:
                    log(f"✅ No new blocks (still at block {new_block:,})")
                
                consecutive_errors = 0  # Reset error counter on success
            else:
                consecutive_errors += 1
                log(f"❌ Run failed with return code {result.returncode}")
                log(f"Error output: {result.stderr[:500]}")  # Log first 500 chars of error
                
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log(f"⚠️ {MAX_CONSECUTIVE_ERRORS} consecutive errors. Stopping continuous sync.")
                    log("Please check logs and fix issues before restarting.")
                    break
            
            # Sleep before next run
            elapsed = (datetime.now() - start_time).total_seconds()
            log(f"Run completed in {elapsed:.1f}s. Sleeping for {SLEEP_SECONDS}s...")
            log("-" * 80)
            time.sleep(SLEEP_SECONDS)
            
        except KeyboardInterrupt:
            log("")
            log("🛑 Stopping continuous sync (user interrupted)")
            break
        except Exception as e:
            consecutive_errors += 1
            log(f"⚠️ Unexpected error: {e}")
            
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log(f"⚠️ {MAX_CONSECUTIVE_ERRORS} consecutive errors. Stopping.")
                break
            
            log("Sleeping for 30s before retry...")
            time.sleep(30)
    
    log("=" * 80)
    log("Continuous sync stopped.")

if __name__ == "__main__":
    main()

```

## scripts/parallel_backfill.sh
```bash
#!/bin/bash
# Parallel backfill script
# Usage: ./parallel_backfill.sh

set -e

# Define ranges
# Current head is approx 2,291,096
# Last synced is 1,970,204
# Gap is ~320k blocks

# 4 Parallel Jobs
# Range 1: 1,970,205 - 2,050,000 (79,795 blocks)
# Range 2: 2,050,001 - 2,130,000 (80,000 blocks)
# Range 3: 2,130,001 - 2,210,000 (80,000 blocks)
# Range 4: 2,210,001 - 2,291,096+ (81,096+ blocks) - will likely hit head

LOG_DIR="logs/backfill"
mkdir -p $LOG_DIR

echo "🚀 Starting Parallel Backfill..."

# Function to run a backfill chunk in background
run_chunk() {
    local job_id=$1
    local start_block=$2
    local end_block=$3
    local log_file="$LOG_DIR/job_${job_id}.log"
    
    echo "  Job $job_id: $start_block - $end_block (Logging to $log_file)"
    
    # Using src.pipeline directly
    # Note: --batch-size 2000 for speed
    nohup python3 -u -m src.pipeline \
        --chain incentiv \
        --logs \
        --decoded-logs \
        --skip-dune \
        --batch-size 2000 \
        --from-block $start_block \
        --to-block $end_block > $log_file 2>&1 &
        
    echo $! > "$LOG_DIR/job_${job_id}.pid"
}

run_chunk 1 1970205 2050000
run_chunk 2 2050001 2130000
run_chunk 3 2130001 2210000
run_chunk 4 2210001 2300000 # Go slightly past current head to be safe

echo ""
echo "✅ All jobs started."
echo "Monitor progress with: tail -f logs/backfill/*.log"

```

## scripts/check_sync_status.py
```python
import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Supabase Setup
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# RPC Setup (fallback to env if available, else standard public RPCs or the one in .env)
# Using the variable name from other scripts for consistency
BLOCKSCOUT_RPC = os.environ.get("INCENTIV_BLOCKSCOUT_RPC_URL", "https://eth-mainnet.public.blastapi.io") 
# WAIT, the user is likely on a testnet given "Incentivtestnetdata", let's check .env content again conceptually 
# but I don't need to view it if I trust the env var loading.
# I'll add a fallback strictly for the print out.

def get_current_chain_height():
    rpc_url = os.environ.get("INCENTIV_BLOCKSCOUT_RPC_URL")
    if not rpc_url:
        print("⚠️ INCENTIV_BLOCKSCOUT_RPC_URL not set in .env, checking known variables...")
        # Try to find any likely RPC url
        for k, v in os.environ.items():
            if "RPC" in k and "URL" in k:
                rpc_url = v
                print(f"   Using {k}={v}")
                break
    
    if not rpc_url:
        print("❌ No RPC URL found.")
        return None

    try:
        response = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            },
            timeout=10
        )
        result = response.json()
        if "result" in result:
            return int(result["result"], 16)
        else:
            print(f"❌ RPC Error: {result}")
            return None
    except Exception as e:
        print(f"❌ Error fetching chain height: {e}")
        return None

def get_supabase_stats():
    try:
        # Get Max Block from BLOCKS table (much faster)
        res = supabase.table("blocks") \
            .select("number") \
            .order("number", desc=True) \
            .limit(1) \
            .execute()
        
        max_block = res.data[0]['number'] if res.data else 0
        
        # Get Min Block
        res_min = supabase.table("blocks") \
            .select("number") \
            .order("number", desc=False) \
            .limit(1) \
            .execute()
            
        min_block = res_min.data[0]['number'] if res_min.data else 0

        return max_block, min_block
    except Exception as e:
        print(f"❌ Error querying Supabase: {e}")
        return 0, 0

def main():
    print("🔍 Checking Sync Status...")
    
    chain_height = get_current_chain_height()
    db_max, db_min = get_supabase_stats()
    
    print("-" * 40)
    print(f"🔗 Live Chain Height:      {chain_height:,}" if chain_height else "🔗 Live Chain Height:      Unknown")
    print(f"🗄️  Supabase Max Block:     {db_max:,}")
    print(f"🗄️  Supabase Min Block:     {db_min:,}")
    print("-" * 40)
    
    if chain_height:
        diff = chain_height - db_max
        if diff <= 5: # Arbitrary small number for "synced"
            print(f"✅ SYNCHRONIZED (Only {diff} blocks behind head, likely buffering)")
        elif diff < 1000:
            print(f"⚠️  CATCHING UP ({diff:,} blocks behind)")
        else:
            print(f"🛑 LAG DETECTED ({diff:,} blocks behind)")
            
    # Check coverage rough estimation
    if db_max > 0:
        print(f"📊 Data spans from block {db_min:,} to {db_max:,}")

if __name__ == "__main__":
    main()

```

## src/pipeline.py
```python
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
load_dotenv()

from src.config import load_yaml
from src.extractors.blockscout import BlockscoutExtractor
from src.handlers.dlq import DeadLetterQueue
from src.loaders.dune import DuneLoader
from src.transformers.logs import normalize_logs
from src.transformers.decoded_logs import decode_logs
from src.transformers.blocks import normalize_blocks
from src.transformers.transactions import normalize_transactions
from src.extractors.transactions import TransactionsExtractor


def load_state(path: Path) -> Dict[str, int]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except:
            return {}
    return {}


def save_state(path: Path, state: Dict[str, int]) -> None:
    path.write_text(json.dumps(state))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incentiv Blockscout ETL")
    parser.add_argument("--chain", default="incentiv")
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--state-file", default="state.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--blocks", action="store_true", help="Extract blocks")
    parser.add_argument("--transactions", action="store_true", help="Extract transactions")
    parser.add_argument("--logs", action="store_true", help="Extract logs (default if no flags)")
    parser.add_argument("--decoded-logs", action="store_true", help="Decode logs and upload decoded table")
    parser.add_argument("--decoded-logs-file", type=str, default=None, help="Decode logs from a CSV export instead of extracting")
    parser.add_argument("--skip-dune", action="store_true", help="Skip Dune uploads and save data locally instead")
    parser.add_argument("--batch-size", type=int, default=None, help="Override batch size from config")
    return parser.parse_args()


# Suppress pandera FutureWarning
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pandera")


def load_logs_from_csv(path: Path) -> List[Dict]:
    df = pd.read_csv(path)
    logs: List[Dict] = []
    for row in df.itertuples(index=False):
        topics = []
        for topic in [getattr(row, "topic0", None), getattr(row, "topic1", None), getattr(row, "topic2", None), getattr(row, "topic3", None)]:
            if pd.isna(topic) or not topic:
                continue
            topics.append(str(topic))
        
        # Determine block_timestamp
        block_timestamp_raw = getattr(row, "block_timestamp", None)
        if pd.isna(block_timestamp_raw) or not block_timestamp_raw:
            block_timestamp = datetime.utcfromtimestamp(0)
        else:
            block_timestamp = pd.to_datetime(block_timestamp_raw, utc=True)
            if hasattr(block_timestamp, "to_pydatetime"):
                block_timestamp = block_timestamp.to_pydatetime()

        logs.append(
            {
                "blockNumber": hex(int(getattr(row, "block_number"))),
                "transactionHash": getattr(row, "tx_hash"),
                "logIndex": hex(int(getattr(row, "log_index"))),
                "address": getattr(row, "address"),
                "topics": topics,
                "data": getattr(row, "data"),
                "block_timestamp": block_timestamp,
            }
        )
    return logs


def enrich_logs_with_timestamps(logs: List[Dict], blocks: Dict[int, Dict]) -> None:
    for log in logs:
        block_number = int(log["blockNumber"], 16)
        block = blocks.get(block_number)
        if not block:
            log["block_timestamp"] = datetime.utcfromtimestamp(0)
            continue
        log["block_timestamp"] = datetime.utcfromtimestamp(int(block["timestamp"], 16))


def run_logs_etl(args: argparse.Namespace, extractor: BlockscoutExtractor, dune_loader: DuneLoader, state: Dict, state_path: Path) -> None:
    events = load_yaml("config/events.yaml")
    destinations = load_yaml("config/destinations.yaml")
    
    event_config = events[args.chain]
    dune_cfg = destinations["dune"]
    table_name = dune_cfg["tables"]["logs"]
    decoded_table = dune_cfg["tables"].get("decoded_logs", "incentiv_decoded_logs")

    last_block = state.get("last_block", 0)
    safe_block = args.to_block if args.to_block is not None else extractor.get_safe_block_number()
    start_block = args.from_block if args.from_block is not None else last_block + 1

    if start_block > safe_block:
        print("No new blocks to process (Logs).")
        return

    print(f"Logs Extraction range: {start_block} to {safe_block}")
    dlq = DeadLetterQueue()

    contracts = {k: v.lower() for k, v in event_config["contracts"].items() if v}
    topics = {k: v.lower() for k, v in event_config["topics"].items() if v}

    if not topics:
        raise RuntimeError("Missing topics in config/events.yaml")

    for start in range(start_block, safe_block + 1, extractor.batch_size):
        end = min(start + extractor.batch_size - 1, safe_block)
        for contract_name, address in contracts.items():
            try:
                topic_list = list(topics.values())
                print(f"  Scanning {contract_name} for {len(topic_list)} topics in {start}-{end}...")
                logs = extractor.get_logs(address, [topic_list], start, end)
                if not logs:
                    continue
                
                print(f"  🔥 Found {len(logs)} logs for {contract_name}!")
                block_numbers = sorted(list(set([int(log["blockNumber"], 16) for log in logs])))
                print(f"  Fetching {len(block_numbers)} blocks for timestamps...")
                blocks = extractor.get_blocks_by_number(block_numbers)
                
                print(f"  Enriching logs...")
                enrich_logs_with_timestamps(logs, blocks)
                
                print(f"  Normalizing logs into DataFrame...")
                df = normalize_logs(logs, chain=args.chain)
                
                if args.skip_dune:
                    # Save locally instead of uploading to Dune
                    backup_dir = Path("backups/logs")
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    backup_file = backup_dir / f"{contract_name}_{start}_{end}.csv"
                    df.to_csv(backup_file, index=False)
                    print(f"  💾 Saved {len(df)} logs locally to {backup_file}")
                elif args.dry_run:
                    print(f"  [DRY RUN] {contract_name} -> {len(df)} logs")
                else:
                    print(f"  Uploading {len(df)} logs to Dune table {table_name}...")
                    dune_loader.upload_dataframe(
                        table_name=table_name,
                        df=df,
                        description=f"{args.chain} logs from Blockscout",
                        dedupe_columns=["block_number", "tx_hash", "log_index"],
                    )

                if args.decoded_logs:
                    print("  Decoding logs using ABI definitions...")
                    decoded_tables = decode_logs(
                        logs=logs,
                        chain=args.chain,
                        abi_dir=Path("config/abis"),
                    )
                    
                    # Upload each table separately
                    for table_key, decoded_df in decoded_tables.items():
                        decoded_table_name = dune_cfg["tables"].get(table_key)
                        if not decoded_table_name:
                            print(f"  ⚠️ No table mapping for {table_key}, skipping...")
                            continue
                        
                        if args.skip_dune:
                            # Save decoded logs locally
                            backup_dir = Path("backups/decoded_logs")
                            backup_dir.mkdir(parents=True, exist_ok=True)
                            backup_file = backup_dir / f"{table_key}_{start}_{end}.csv"
                            decoded_df.to_csv(backup_file, index=False)
                            print(f"  💾 Saved {len(decoded_df)} decoded events to {backup_file}")
                        elif args.dry_run:
                            print(f"  [DRY RUN] {table_key} -> {len(decoded_df)} events")
                        else:
                            print(f"  Uploading {len(decoded_df)} events to {decoded_table_name}...")
                            try:
                                dune_loader.upload_dataframe(
                                    table_name=decoded_table_name,
                                    df=decoded_df,
                                    description=f"{args.chain} {table_key} from Blockscout",
                                    dedupe_columns=["block_number", "tx_hash", "log_index"],
                                )
                                print(f"  ✅ Uploaded {len(decoded_df)} events to {decoded_table_name}")
                            except Exception as e:
                                print(f"  ⚠️ Failed to upload {table_key}: {e}")
                                # Continue with other tables even if one fails
                
                print(f"  ✅ Log processing complete for {contract_name}.")
            except Exception as exc:
                print(f"  ❌ Failed to process logs for {contract_name}: {exc}")
                dlq.send(
                    record={"contract": contract_name, "topics": list(topics.keys())},
                    error=exc,
                    context={"from_block": start, "to_block": end},
                )
                if not args.dry_run:
                    print(f"  ⚠️ Aborting batch {start}-{end} due to failure. State will NOT advance.")
                    return # Stop the entire run so we don't skip this block

        state["last_block"] = end
        save_state(state_path, state)


def run_blocks_transactions_etl(args: argparse.Namespace, extractor: BlockscoutExtractor, dune_loader: DuneLoader, state: Dict, state_path: Path) -> None:
    destinations = load_yaml("config/destinations.yaml")
    dune_cfg = destinations["dune"]
    
    # Tables
    blocks_table = dune_cfg["tables"].get("blocks", "incentiv_blocks")
    txs_table = dune_cfg["tables"].get("transactions", "incentiv_transactions")

    # Use separate state for chain data to avoid conflict with logs if needed, 
    # but for now let's use last_chain_block
    last_block = state.get("last_chain_block", 0)

    safe_block = args.to_block if args.to_block is not None else extractor.get_safe_block_number()
    start_block = args.from_block if args.from_block is not None else last_block + 1

    if start_block > safe_block:
        print("No new blocks to process (Chain).")
        return

    # Increase batch size for blocks (metadata), keep receipts small separately
    batch_size = 10 
    
    print(f"Chain Extraction range: {start_block} to {safe_block}")

    tx_extractor = TransactionsExtractor(extractor)

    for start in range(start_block, safe_block + 1, batch_size):
        end = min(start + batch_size - 1, safe_block)
        try:
            block_numbers = list(range(start, end + 1))
            
            # Fetch blocks with transactions
            blocks_map = extractor.get_blocks_by_number(block_numbers, include_transactions=True)
            blocks = list(blocks_map.values())
            
            if not blocks:
                continue
                
            # Process Blocks
            if args.blocks:
                df_blocks = normalize_blocks(blocks, chain=args.chain)
                if args.dry_run:
                    print(f"Blocks {start}-{end} -> {len(df_blocks)} records")
                else:
                    dune_loader.upload_dataframe(
                        table_name=blocks_table,
                        df=df_blocks,
                        description=f"{args.chain} blocks",
                        dedupe_columns=["block_number", "hash"],
                    )

            # Process Transactions
            if args.transactions:
                tx_hashes = [
                    tx["hash"]
                    for block in blocks
                    if isinstance(block.get("transactions"), list)
                    for tx in block["transactions"]
                    if isinstance(tx, dict) and "hash" in tx
                ]
                
                receipts_by_hash = {}
                if tx_hashes:
                    print(f"  Fetching {len(tx_hashes)} receipts...")
                    receipts_by_hash = tx_extractor.get_transaction_receipts(tx_hashes)

                df_txs = normalize_transactions(blocks, chain=args.chain, receipts_by_hash=receipts_by_hash)
                if args.dry_run:
                    print(f"Txs {start}-{end} -> {len(df_txs)} records")
                else:
                    dune_loader.upload_dataframe(
                        table_name=txs_table,
                        df=df_txs,
                        description=f"{args.chain} transactions",
                        dedupe_columns=["hash", "block_number"],
                    )
            
            state["last_chain_block"] = end
            save_state(state_path, state)
            
        except Exception as e:
            print(f"Error processing chain batch {start}-{end}: {e}")
            if not args.dry_run:
                 raise e


def main() -> None:
    args = parse_args()
    
    chains = load_yaml("config/chains.yaml")
    destinations = load_yaml("config/destinations.yaml")
    chain_config = chains[args.chain]
    dune_cfg = destinations["dune"]

    extractor = BlockscoutExtractor(
        base_url=chain_config["blockscout_base_url"],
        rpc_url=chain_config["blockscout_rpc_url"],
        confirmations=int(chain_config["confirmations"]),
        batch_size=args.batch_size if args.batch_size else int(chain_config["batch_size"]),
        rate_limit_per_second=float(chain_config["rate_limit_per_second"]),
    )

    dune_loader = DuneLoader(
        api_key=dune_cfg["api_key"], 
        base_url=dune_cfg["base_url"],
        namespace=dune_cfg.get("namespace", "surgence_lab")
    )

    state_path = Path(args.state_file)
    state = load_state(state_path)

    if args.decoded_logs_file:
        print(f"📄 Loading logs from {args.decoded_logs_file} for decoding...")
        logs = load_logs_from_csv(Path(args.decoded_logs_file))
        decoded_table = dune_cfg["tables"].get("decoded_logs", "incentiv_decoded_logs")
        print("  Decoding logs using ABI definitions...")
        decoded_df = decode_logs(logs=logs, chain=args.chain, abi_dir=Path("config/abis"))
        print(f"  Uploading {len(decoded_df)} decoded logs to Dune table {decoded_table}...")
        if args.dry_run:
            print(f"  [DRY RUN] decoded logs file -> {len(decoded_df)} decoded logs")
        else:
            dune_loader.upload_dataframe(
                table_name=decoded_table,
                df=decoded_df,
                description=f"{args.chain} decoded logs from CSV",
                dedupe_columns=["block_number", "tx_hash", "log_index"],
            )
        return

    # Run logic:
    # 1. If --blocks or --transactions, run chain ETL
    # 2. If --logs OR no flags provided at all, run logs ETL
    
    should_run_chain = args.blocks or args.transactions
    should_run_logs = args.logs or args.decoded_logs or not (args.blocks or args.transactions)

    if should_run_chain:
        print("🛠️ Starting Blocks/Transactions ETL...")
        run_blocks_transactions_etl(args, extractor, dune_loader, state, state_path)

    if should_run_logs:
        print("🛠️ Starting Logs ETL...")
        run_logs_etl(args, extractor, dune_loader, state, state_path)


if __name__ == "__main__":
    main()

```

## dashboard/src/lib/api.ts
```typescript

import { supabase } from './supabase'

export async function getRecentActivity() {
    const { data, error } = await supabase
        .from('decoded_events')
        .select('*')
        .order('timestamp', { ascending: false })
        .limit(10)

    if (error) {
        console.error('Error fetching activity:', error)
        return []
    }

    return data.map((event) => ({
        hash: event.transaction_hash,
        from: event.params.sender || event.params.from || "0x...",
        to: event.params.recipient || event.params.to || event.contract_address,
        value: "0.00", // placeholder as value parsing logic is complex
        token: "INC",
        timestamp: new Date(event.timestamp).toLocaleTimeString(),
        type: event.event_name.toLowerCase()
    }))
}

export async function getStats() {
    // Get total count
    const { count, error } = await supabase
        .from('decoded_events')
        .select('*', { count: 'exact', head: true })

    // Get 24h count
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
    const { count: count24h } = await supabase
        .from('decoded_events')
        .select('*', { count: 'exact', head: true })
        .gt('timestamp', yesterday)

    return {
        totalTransactions: count || 0,
        activeWallets: 0, // Hard to calculate distinct without RPC
        totalVolume: 0, // Hard to sum JSONB without RPC
        transactions24h: count24h || 0
    }
}

export async function getChartData() {
    // Ideally we use an RPC function for this.
    // For now, let's fetch last 1000 events and group client side (inefficient but works for small scale MVP)
    const { data } = await supabase
        .from('decoded_events')
        .select('timestamp')
        .order('timestamp', { ascending: false })
        .limit(2000)

    if (!data) return []

    const days: Record<string, number> = {}
    data.forEach(d => {
        const date = new Date(d.timestamp).toLocaleDateString("en-US", { month: "short", day: "numeric" })
        days[date] = (days[date] || 0) + 1
    })

    return Object.entries(days)
        .map(([date, count]) => ({ date, transactions: count }))
        .reverse()
}

```
