
import csv
import requests
import time
import os
import io

DUNE_API_KEY = "3nKpTZrrziBToMPOY7z2nybU8c6L3Our"
DUNE_API_BASE = "https://api.dune.com/api/v1"

def upload_csv_file(file_path, table_name, description=""):
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    print(f"Reading {file_path}...")
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    total_rows = len(rows)
    print(f"Loaded {total_rows} rows. Starting upload to {table_name}...")

    # Chunk size
    CHUNK_SIZE = 500
    url = f"{DUNE_API_BASE}/table/upload/csv"
    # Force identity encoding to prevent gzip issues
    headers = {
        "X-Dune-Api-Key": DUNE_API_KEY,
        "Accept-Encoding": "identity"
    }

    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        
        # Convert chunk to CSV string
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=chunk[0].keys())
        writer.writeheader()
        writer.writerows(chunk)
        csv_content = output.getvalue()

        files = {"data": (f"{table_name}.csv", csv_content, "text/csv")}
        form_data = {
            "table_name": table_name,
            "description": description or f"Incentiv {table_name} data",
            "is_private": "false"
        }

        print(f"  Uploading chunk {i//CHUNK_SIZE + 1}/{(total_rows + CHUNK_SIZE - 1)//CHUNK_SIZE} ({len(chunk)} rows)...")
        
        try:
            resp = requests.post(url, files=files, data=form_data, headers=headers)
            
            if resp.status_code == 200:
                print(f"    ✅ Success")
            else:
                print(f"    ❌ Failed: {resp.status_code}")
                try:
                    print(f"    Response: {resp.text}")
                except Exception as e:
                    print(f"    Could not read response text: {e}")
                    print(f"    Raw content: {resp.content}")

        except Exception as e:
            print(f"    ❌ Exception: {e}")
        
        # Rate limit protection
        time.sleep(1)

def main():
    # 1. Bridge Transfers
    upload_csv_file("incentiv_bridge_transfers_backup.csv", "incentiv_bridge_transfers", "Incentiv bridge inflows")
    
    # 2. Token Transfers
    upload_csv_file("incentiv_token_transfers_backup.csv", "incentiv_token_transfers", "Incentiv ERC20 token transfers")

    # 3. DEX Swaps (check if backup exists, might not if it crashed during token transfers upload)
    upload_csv_file("incentiv_dex_swaps_backup.csv", "incentiv_dex_swaps", "Incentiv DEX swaps")
    
    # 4. Active Wallets
    upload_csv_file("incentiv_active_wallets_backup.csv", "incentiv_active_wallets", "Incentiv active wallets")

if __name__ == "__main__":
    main()
