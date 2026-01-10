
import os
import requests
import json
from dotenv import load_dotenv
from typing import List, Dict, Any

load_dotenv()
RPC_URL = os.getenv("INCENTIV_BLOCKSCOUT_RPC_URL")
MAILBOX = os.getenv("INCENTIV_MAILBOX_ADDRESS")
ROUTER = os.getenv("INCENTIV_HYPERLANE_ROUTER_ADDRESS")
DEX_POOL = "0xf9884c2A1749b0a02ce780aDE437cBaDFA3a961D"

CONTRACTS = {
    "mailbox": MAILBOX,
    "hyperlane_router": ROUTER,
    "dex_pool": DEX_POOL
}

TOPICS = [
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", # transfer
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925", # approval
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67", # swap_v3
    "0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6", # received_transfer_remote
    "0x0d381c2a574ae8f04e213db7cfb4df8df712cdbd427d9868ffef380660ca6574"  # process
]

def get_rpc_logs(address: str, from_block: int, to_block: int) -> int:
    """Recursively fetch and count logs to avoid 413."""
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "address": address,
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [TOPICS]
        }]
    }
    try:
        resp = requests.post(RPC_URL, json=payload, timeout=60).json()
        if "error" in resp:
            if "too large" in str(resp["error"]).lower() or "413" in str(resp["error"]).lower():
                raise RuntimeError("Too Large")
            return 0
        return len(resp.get("result", []))
    except Exception as e:
        if from_block < to_block:
            mid = (from_block + to_block) // 2
            return get_rpc_logs(address, from_block, mid) + get_rpc_logs(address, mid + 1, to_block)
        return 0

def check_range(name: str, start: int, end: int):
    print(f"Checking range {start} to {end}...")
    total = 0
    for c_name, addr in CONTRACTS.items():
        if not addr: continue
        count = get_rpc_logs(addr, start, end)
        print(f"  {c_name}: {count}")
        total += count
    return total

def main():
    h_resp = requests.post(RPC_URL, json={"id":1,"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}).json()
    head = int(h_resp["result"], 16)
    
    ranges = [
        (1, 100000),
        (100001, 500000),
        (500001, 1000000),
        (1000001, 1500000),
        (1500001, head)
    ]
    
    results = []
    print(f"Starting Proof Verification (Head: {head})")
    for s, e in ranges:
        count = check_range(f"{s}-{e}", s, e)
        results.append({"range": f"{s}-{e}", "count": count})
    
    print("\nFinal Results:")
    print(json.dumps(results, indent=2))
    
if __name__ == "__main__":
    main()
