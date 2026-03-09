import os
import sys
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.loaders.neon import NeonLoader

load_dotenv()
load_dotenv('.env.neon')

loader = NeonLoader()

print("=== INVESTIGATING BLOCKS TABLE ===\n")

print("1. Total row count:")
res_count = loader.query("SELECT COUNT(*) FROM blocks")
print(f"   Total rows: {res_count[0][0]:,}\n")

print("2. Block number range:")
res_minmax = loader.query("SELECT MIN(number), MAX(number) FROM blocks")
min_block = res_minmax[0][0] or 0
max_block = res_minmax[0][1] or 0
print(f"   Min: {min_block:,}")
print(f"   Max: {max_block:,}")
print(f"   Expected if continuous: {max_block - min_block + 1:,}\n")

print("3. Rows with NON-NULL timestamps:")
res_non_null = loader.query("SELECT COUNT(*) FROM blocks WHERE timestamp IS NOT NULL")
print(f"   Count: {res_non_null[0][0]:,}\n")

print("4. Sample of RECENT blocks (should have data):")
recent = loader.query("SELECT number, hash, timestamp, parent_hash FROM blocks ORDER BY number DESC LIMIT 3")
for row in recent:
    h = row[1][:20] + "..." if row[1] else "NULL"
    ph = "YES" if row[3] else "NULL"
    print(f"   Block {row[0]}: hash={h}, timestamp={row[2]}, parent_hash={ph}")

print("\n5. Sample of OLD blocks (from migration):")
old = loader.query("SELECT number, hash, timestamp, parent_hash FROM blocks WHERE number >= 938365 AND number <= 938370")
for row in old:
    h = row[1] if row[1] else "NULL"
    if len(h) > 20: h = h[:20] + "..."
    ph = "YES" if row[3] else "NULL"
    print(f"   Block {row[0]}: hash={h}, timestamp={row[2]}, parent_hash={ph}")

loader.close()
