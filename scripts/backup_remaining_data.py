#!/usr/bin/env python3
"""
Backup script to extract and save all remaining blockchain data locally.
This ensures no data is lost while waiting for Dune API credits.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors.blockscout import BlockscoutExtractor
from src.transformers.decoded_logs import decode_logs
from src.transformers.logs import normalize_logs, enrich_logs_with_timestamps
from src.config import load_yaml

load_dotenv()

def backup_remaining_data():
    """Extract and backup all remaining data to CSV files."""
    
    # Load config
    events = load_yaml("config/events.yaml")
    event_config = events["incentiv"]
    
    # Load state
    with open("state.json") as f:
        state = json.load(f)
    
    last_block = state["last_block"]
    
    # Initialize extractor
    extractor = BlockscoutExtractor("incentiv")
    current_head = extractor.get_safe_block_number()
    
    print(f"🔄 Backing up data from block {last_block + 1:,} to {current_head:,}")
    print(f"Total blocks to backup: {current_head - last_block:,}")
    print()
    
    # Prepare backup directory
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Extract contracts and topics
    contracts = {k: v.lower() for k, v in event_config["contracts"].items() if v}
    topics = {k: v.lower() for k, v in event_config["topics"].items() if v}
    topic_list = list(topics.values())
    
    all_logs = []
    batch_size = 100
    
    # Extract logs in batches
    for start in range(last_block + 1, current_head + 1, batch_size):
        end = min(start + batch_size - 1, current_head)
        
        print(f"Processing blocks {start:,} - {end:,}...")
        
        for contract_name, address in contracts.items():
            try:
                logs = extractor.get_logs(address, [topic_list], start, end)
                if logs:
                    print(f"  Found {len(logs)} logs from {contract_name}")
                    all_logs.extend(logs)
            except Exception as e:
                print(f"  Error extracting {contract_name}: {e}")
    
    if not all_logs:
        print("No logs found in remaining blocks")
        return
    
    print(f"\n✅ Extracted {len(all_logs)} total logs")
    print("Enriching with timestamps...")
    
    # Get unique block numbers
    block_numbers = sorted(list(set([int(log["blockNumber"], 16) for log in all_logs])))
    print(f"Fetching {len(block_numbers)} blocks for timestamps...")
    blocks = extractor.get_blocks_by_number(block_numbers)
    
    # Enrich logs
    enrich_logs_with_timestamps(all_logs, blocks)
    
    # Save raw logs
    raw_logs_file = backup_dir / f"raw_logs_{timestamp}.json"
    with open(raw_logs_file, "w") as f:
        json.dump(all_logs, f, indent=2)
    print(f"✅ Saved raw logs to {raw_logs_file}")
    
    # Normalize logs to DataFrame
    print("Normalizing logs...")
    logs_df = normalize_logs(all_logs, chain="incentiv")
    logs_csv = backup_dir / f"logs_{timestamp}.csv"
    logs_df.to_csv(logs_csv, index=False)
    print(f"✅ Saved {len(logs_df)} normalized logs to {logs_csv}")
    
    # Decode logs
    print("Decoding logs...")
    decoded_tables = decode_logs(all_logs, "incentiv", Path("config/abis"))
    
    for table_name, decoded_df in decoded_tables.items():
        decoded_csv = backup_dir / f"{table_name}_{timestamp}.csv"
        decoded_df.to_csv(decoded_csv, index=False)
        print(f"✅ Saved {len(decoded_df)} {table_name} events to {decoded_csv}")
    
    # Create summary
    summary = {
        "backup_timestamp": timestamp,
        "block_range": {
            "start": last_block + 1,
            "end": current_head,
            "total_blocks": current_head - last_block
        },
        "total_logs": len(all_logs),
        "decoded_tables": {
            table_name: len(df) for table_name, df in decoded_tables.items()
        },
        "files": {
            "raw_logs": str(raw_logs_file),
            "normalized_logs": str(logs_csv),
            "decoded_events": [str(backup_dir / f"{t}_{timestamp}.csv") for t in decoded_tables.keys()]
        }
    }
    
    summary_file = backup_dir / f"backup_summary_{timestamp}.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n🎉 Backup complete!")
    print(f"Summary saved to {summary_file}")
    print(f"\nTo upload to Dune later, use:")
    print(f"  python3 -m src.pipeline --decoded-logs-file {logs_csv}")

if __name__ == "__main__":
    backup_remaining_data()
