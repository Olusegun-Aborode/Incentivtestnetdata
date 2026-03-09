#!/usr/bin/env python3
"""
Analyze backup coverage - what block ranges do we have?
"""

import os
import re
from pathlib import Path

def analyze_backup_coverage(backup_dir, file_pattern):
    """Analyze what block ranges are covered by backup CSVs"""
    files = list(Path(backup_dir).glob(file_pattern))
    
    if not files:
        return None, None, 0
    
    block_ranges = []
    for f in files:
        # Extract block range from filename like: blocks_938365_938374.csv
        match = re.search(r'_(\d+)_(\d+)\.csv', f.name)
        if match:
            start = int(match.group(1))
            end = int(match.group(2))
            block_ranges.append((start, end))
    
    if not block_ranges:
        return None, None, 0
    
    block_ranges.sort()
    min_block = block_ranges[0][0]
    max_block = block_ranges[-1][1]
    total_files = len(block_ranges)
    
    return min_block, max_block, total_files

print("=" * 60)
print("BACKUP COVERAGE ANALYSIS")
print("=" * 60)

# Analyze each backup type
backup_types = [
    ('blocks', 'backups/blocks', 'blocks_*.csv'),
    ('transactions', 'backups/transactions', 'transactions_*.csv'),
    ('logs', 'backups/logs', 'logs_*.csv'),
    ('decoded_logs', 'backups/decoded_logs', 'decoded_logs_*.csv'),
]

for name, directory, pattern in backup_types:
    min_b, max_b, count = analyze_backup_coverage(directory, pattern)
    
    print(f"\n📦 {name.upper()}:")
    if min_b:
        print(f"   Files: {count:,}")
        print(f"   Range: {min_b:,} → {max_b:,}")
        print(f"   Span: {max_b - min_b + 1:,} blocks")
    else:
        print(f"   No files found")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print("\nWe have local CSV backups covering blocks ~938k → ~1.14M")
print("This means we can:")
print("  1. Import these backups to Neon (fast)")
print("  2. Only backfill the gaps:")
print("     - Gap 1: Block 0 → 938k")
print("     - Gap 2: Block 1.14M → 2.29M (current)")
