#!/usr/bin/env python3
"""
Backfill blocks and transactions metadata
Strategy: Process in chunks, using existing block numbers as guide
"""

import subprocess
import sys
from pathlib import Path

MIN_BLOCK = 2041707
MAX_BLOCK = 2610651
CHUNK_SIZE = 10000

LOG_DIR = Path("logs/backfill_blocks_txs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

print("🚀 Starting Block & Transaction Metadata Backfill...")
print(f"Range: {MIN_BLOCK:,} to {MAX_BLOCK:,}")
print(f"Chunk size: {CHUNK_SIZE:,}")
print("")

start = MIN_BLOCK
while start <= MAX_BLOCK:
    end = min(start + CHUNK_SIZE - 1, MAX_BLOCK)
    
    print(f"📦 Processing blocks {start:,} to {end:,}...")
    
    # Extract blocks and transactions and push to Neon
    extract_log = LOG_DIR / f"extract_{start}_{end}.log"
    with open(extract_log, "w") as f:
        result = subprocess.run([
            "python3", "-m", "src.pipeline",
            "--chain", "incentiv",
            "--blocks",
            "--transactions",
            "--all-activity",
            "--neon",
            "--from-block", str(start),
            "--to-block", str(end),
            "--batch-size", "500"
        ], stdout=f, stderr=subprocess.STDOUT)
    
    if result.returncode != 0:
        print(f"❌ Extraction failed for chunk {start}-{end}")
        print(f"   Check log: {extract_log}")
        sys.exit(1)
    

    
    print(f"✅ Completed chunk {start:,}-{end:,}")
    print("-" * 40)
    
    start += CHUNK_SIZE

print("")
print("🎉 Backfill Complete!")
print("Run 'python3 scripts/audit_data.py' to verify.")
