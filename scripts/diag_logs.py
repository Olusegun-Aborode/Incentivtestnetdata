import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

RPC_URL = os.getenv("INCENTIV_BLOCKSCOUT_RPC_URL")
MAILBOX = os.getenv("INCENTIV_MAILBOX_ADDRESS")

def get_logs(address, from_block, to_block):
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [
            {
                "address": address.lower() if address else None,
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
            }
        ],
    }
    resp = requests.post(RPC_URL, json=payload)
    return resp.json().get("result", [])

def main():
    latest_hex = requests.post(RPC_URL, json={"id":1, "jsonrpc":"2.0", "method":"eth_blockNumber", "params":[]}).json()["result"]
    latest = int(latest_hex, 16)
    print(f"Latest block: {latest}")
    
    # Check last 5000 blocks for Mailbox logs
    print(f"Checking Mailbox ({MAILBOX}) logs in last 5000 blocks...")
    logs = get_logs(MAILBOX, latest - 5000, latest)
    print(f"Found {len(logs)} logs for Mailbox")
    
    # Check last 1000 blocks for ANY logs (to verify method)
    print(f"Checking ANY logs in last 100 blocks...")
    logs = get_logs(None, latest - 100, latest)
    print(f"Found {len(logs)} logs in total for last 100 blocks")
    if logs:
        print(f"Example log address: {logs[0]['address']}")

if __name__ == "__main__":
    main()
