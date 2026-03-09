
import os
import sys
import pandas as pd
import glob
import requests
import json
from dotenv import load_dotenv

load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("❌ Missing credentials")
    sys.exit(1)

headers = {
    "apikey": key,
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def upload(table, data):
    endpoint = f"{url}/rest/v1/{table}"
    # Chunk it
    batch_size = 5000
    total = len(data)
    print(f"Uploading {total} rows to {table}...")
    
    # Convert set/list to list of dicts
    # data is list of dicts
    
    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        try:
            resp = requests.post(endpoint, headers=headers, json=batch)
            if resp.status_code not in [200, 201]:
                print(f"Failed chunk {i}: {resp.text}")
            else:
                print(f"Chunk {i} success")
        except Exception as e:
            print(f"Error {i}: {e}")

def main():
    print("🔍 Scanning CSVs for dependencies...")
    files = glob.glob("backups/decoded_logs/*.csv")
    
    blocks = set()
    txs = set() # (hash, block_number)
    
    for f in files:
        try:
            df = pd.read_csv(f)
            # We need block_number and tx_hash
            # Columns in CSV: block_number, tx_hash / transaction_hash
            
            if 'tx_hash' in df.columns:
                df.rename(columns={'tx_hash': 'transaction_hash'}, inplace=True)
            
            if 'block_number' not in df.columns or 'transaction_hash' not in df.columns:
                continue

            # Add to sets
            for _, row in df[['block_number', 'transaction_hash']].iterrows():
                blocks.add(int(row['block_number']))
                txs.add((str(row['transaction_hash']), int(row['block_number'])))
                
        except Exception:
            pass
            
    print(f"Found {len(blocks)} unique blocks and {len(txs)} unique transactions.")
    
    # Prepare blocks
    # Schema: number, hash (required)
    block_records = [{"number": b, "hash": str(b)} for b in blocks]
    upload("blocks", block_records)
    
    # Prepare transactions
    # Schema: hash, block_number
    # Note: block_number FK to blocks(number) which we just uploaded
    tx_records = [{"hash": t[0], "block_number": t[1]} for t in txs]
    upload("transactions", tx_records)
    
    print("✅ Dependencies populated.")

if __name__ == "__main__":
    main()
