import os
import sys
import pandas as pd
import glob
import requests
import time
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

def upload_batch(table_name, data):
    endpoint = f"{url}/rest/v1/{table_name}"
    try:
        data = data.where(pd.notnull(data), None)
        records = data.to_dict(orient='records')
        resp = requests.post(endpoint, headers=headers, json=records)
        if resp.status_code not in [200, 201]:
            print(f"❌ Upload failed: {resp.text}")
        else:
            print(f"✅ Uploaded {len(records)} rows to {table_name}")
    except Exception as e:
        print(f"❌ Error uploading: {e}")

def upload_blocks_txs():
    # Check for files modified in last 10 minutes
    now = time.time()
    cutoff = now - (10 * 60)
    
    # Upload blocks
    block_files = glob.glob("backups/blocks/*.csv")
    recent_blocks = [f for f in block_files if os.path.getmtime(f) > cutoff]
    
    if recent_blocks:
        print(f"Found {len(recent_blocks)} recent block files.")
        for f in recent_blocks:
            try:
                df = pd.read_csv(f)
                # Rename columns to match Supabase schema
                column_map = {
                    'block_number': 'number',
                    'block_hash': 'hash'
                }
                df.rename(columns=column_map, inplace=True)
                
                # Keep only columns that exist in Supabase
                required = ['number', 'hash', 'parent_hash', 'timestamp', 'gas_used', 'gas_limit', 'base_fee_per_gas']
                available = [c for c in required if c in df.columns]
                df = df[available]
                
                upload_batch("blocks", df)
            except Exception as e:
                print(f"Error processing {f}: {e}")
    
    # Upload transactions
    tx_files = glob.glob("backups/transactions/*.csv")
    recent_txs = [f for f in tx_files if os.path.getmtime(f) > cutoff]
    
    if recent_txs:
        print(f"Found {len(recent_txs)} recent transaction files.")
        for f in recent_txs:
            try:
                df = pd.read_csv(f)
                # Keep only columns that exist in Supabase
                required = ['hash', 'block_number', 'from_address', 'to_address', 'value', 'gas_price', 'gas_used', 'input_data', 'status', 'timestamp']
                available = [c for c in required if c in df.columns]
                df = df[available]
                
                upload_batch("transactions", df)
            except Exception as e:
                print(f"Error processing {f}: {e}")

if __name__ == "__main__":
    upload_blocks_txs()
