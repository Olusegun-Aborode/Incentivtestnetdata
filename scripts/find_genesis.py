#!/usr/bin/env python3
"""
Find genesis block and check what data we have
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

rpc_url = os.getenv("INCENTIV_BLOCKSCOUT_RPC_URL")

print("=" * 60)
print("FINDING GENESIS BLOCK")
print("=" * 60)

# Check block 0
print("\n🔍 Checking block 0...")
response = requests.post(
    rpc_url,
    json={"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["0x0", True], "id": 1}
)

result = response.json().get("result")
if result:
    timestamp = int(result["timestamp"], 16)
    date = datetime.fromtimestamp(timestamp)
    print(f"✅ Genesis Block 0 exists!")
    print(f"   Hash: {result['hash']}")
    print(f"   Timestamp: {timestamp}")
    print(f"   Date: {date}")
    print(f"   Transactions: {len(result.get('transactions', []))}")
    genesis = 0
else:
    print("❌ Block 0 doesn't exist")
    print("\n🔍 Checking block 1...")
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["0x1", True], "id": 1}
    )
    result = response.json().get("result")
    if result:
        timestamp = int(result["timestamp"], 16)
        date = datetime.fromtimestamp(timestamp)
        print(f"✅ Genesis Block 1 exists!")
        print(f"   Hash: {result['hash']}")
        print(f"   Timestamp: {timestamp}")
        print(f"   Date: {date}")
        genesis = 1
    else:
        print("❌ Block 1 doesn't exist either!")
        genesis = None

print("\n" + "=" * 60)
print(f"GENESIS BLOCK: {genesis}")
print("=" * 60)
