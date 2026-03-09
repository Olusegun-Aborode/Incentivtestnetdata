import os
import sys
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.loaders.neon import NeonLoader

load_dotenv()
load_dotenv('.env.neon')

loader = NeonLoader()

# Check one of the blocks we just uploaded
res = loader.query("SELECT hash, timestamp, parent_hash, gas_used FROM blocks WHERE number = %s LIMIT 1", (938365,))
if res:
    block = res[0]
    print(f"Block 938365:")
    print(f"  hash: {block[0]}")
    print(f"  timestamp: {block[1]}")
    print(f"  parent_hash: {block[2]}")
    print(f"  gas_used: {block[3]}")
    print(f"  ✅ HAS METADATA!" if block[1] else "  ❌ STILL NULL")
else:
    print("Block not found")

loader.close()

