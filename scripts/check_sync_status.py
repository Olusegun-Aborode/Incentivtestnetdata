import os
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.loaders.neon import NeonLoader

load_dotenv()
load_dotenv('.env.neon')

# RPC Setup (fallback to env if available, else standard public RPCs)
BLOCKSCOUT_RPC = os.environ.get("INCENTIV_BLOCKSCOUT_RPC_URL", "https://eth-mainnet.public.blastapi.io") 

def get_current_chain_height():
    rpc_url = os.environ.get("INCENTIV_BLOCKSCOUT_RPC_URL")
    if not rpc_url:
        print("⚠️ INCENTIV_BLOCKSCOUT_RPC_URL not set in .env, checking known variables...")
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

def get_db_stats():
    try:
        loader = NeonLoader()
        res = loader.query("SELECT MIN(number), MAX(number) FROM blocks")
        if res and res[0][0] is not None:
            min_block = res[0][0]
            max_block = res[0][1]
        else:
            min_block, max_block = 0, 0
        loader.close()
        return max_block, min_block
    except Exception as e:
        print(f"❌ Error querying Neon DB: {e}")
        return 0, 0

def main():
    print("🔍 Checking Sync Status...")
    
    chain_height = get_current_chain_height()
    db_max, db_min = get_db_stats()
    
    print("-" * 40)
    print(f"🔗 Live Chain Height:      {chain_height:,}" if chain_height else "🔗 Live Chain Height:      Unknown")
    print(f"🗄️  Neon DB Max Block:      {db_max:,}")
    print(f"🗄️  Neon DB Min Block:      {db_min:,}")
    print("-" * 40)
    
    if chain_height:
        diff = chain_height - db_max
        if diff <= 5: 
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

