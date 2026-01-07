import requests
import json

BLOCKSCOUT_URL = "https://explorer.incentiv.io"
TX_HASH = "0x278f9b7916b16b5274e8a7d615a462ac80811c5b18ae6c3558bcc80167defd50"

USDC_ADDRESS = "0x16e43840d8D79896A389a3De85aB0B0210C05685"
BRIDGE_EVENT_SIG = "0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6"

def get_address_logs():
    # Try RPC-style endpoint which usually supports filtering better
    url = f"{BLOCKSCOUT_URL}/api"
    params = {
        "module": "logs",
        "action": "getLogs",
        "address": USDC_ADDRESS,
        "topic0": BRIDGE_EVENT_SIG,
        "fromBlock": "0",
        "toBlock": "latest",
        "page": 1,
        "offset": 10
    }
    print(f"Fetching logs via RPC endpoint: {url} with params {params}")
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        
        # RPC response format is usually {'status': '1', 'message': 'OK', 'result': [...]}
        print(f"Full response: {data}")
        items = data.get("result", [])
        if items is None:
             print("Result is None")
             return

        print(f"Found {len(items)} logs via RPC")
        
        for i, log in enumerate(items):
            topics = log.get("topics", [])
            print(f"Log {i} Topic0: {topics[0] if topics else 'None'}")
            if topics and topics[0] == BRIDGE_EVENT_SIG:
                print(f"  ✅ MATCH! Tx: {log.get('transactionHash')}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_address_logs()
