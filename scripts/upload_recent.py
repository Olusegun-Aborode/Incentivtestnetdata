import os
import sys
import pandas as pd
import glob
import json
import time
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.loaders.neon import NeonLoader

load_dotenv()
load_dotenv('.env.neon')

def upload_recent():
    loader = NeonLoader()
    
    # Check for files modified in last 10 minutes
    now = time.time()
    cutoff = now - (10 * 60)
    
    files = glob.glob("backups/decoded_logs/*.csv")
    recent_files = [f for f in files if os.path.getmtime(f) > cutoff]
    
    if not recent_files:
        print("No new files to upload.")
        loader.close()
        return

    print(f"Found {len(recent_files)} recent files.")
    
    for f in recent_files:
        try:
            df = pd.read_csv(f)
             # MAPPING LOGIC
            column_map = {
                'address': 'contract_address', 'tx_hash': 'transaction_hash', 'block_timestamp': 'timestamp'
            }
            df.rename(columns=column_map, inplace=True)
            
            standard_cols = ['event_name', 'contract_address', 'block_number', 'transaction_hash', 'log_index', 'timestamp', 'chain', 'extracted_at']
            extra_cols = [c for c in df.columns if c not in standard_cols]
            
            def make_params(row):
                return {k: row[k] for k in extra_cols if pd.notna(row[k])}

            if extra_cols:
                 df['params'] = df.apply(lambda row: json.dumps(make_params(row)), axis=1)
            else:
                 df['params'] = "{}"

            required = ['event_name', 'contract_address', 'block_number', 'transaction_hash', 'log_index', 'params', 'timestamp']
            missing = [c for c in required if c not in df.columns]
            
            if not missing:
                df = df[required]
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors="coerce", utc=True)
                
                # UPLOAD DEPENDENCIES (Blocks/Txs) to ensure FKs
                # Blocks
                if 'block_number' in df.columns:
                    blocks = df['block_number'].unique()
                    block_df = pd.DataFrame({"number": blocks, "hash": [str(b) for b in blocks]})
                    loader.copy_dataframe("blocks", block_df, columns=["number", "hash"])
                
                # Txs
                if 'transaction_hash' in df.columns and 'block_number' in df.columns:
                    tx_df = df[['transaction_hash', 'block_number']].drop_duplicates(subset=['transaction_hash'])
                    tx_df = tx_df.rename(columns={'transaction_hash': 'hash'})
                    loader.copy_dataframe("transactions", tx_df, columns=["hash", "block_number"])
                
                # Upload events
                inserted = loader.copy_dataframe("decoded_events", df, columns=required)
                print(f"✅ Uploaded {inserted} events to Neon DB from {f}")

        except Exception as e:
            print(f"Error processing {f}: {e}")

    loader.close()

if __name__ == "__main__":
    upload_recent()
