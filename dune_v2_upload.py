
import requests
import csv
import json
import time
import os

# Configuration
API_KEY = "3nKpTZrrziBToMPOY7z2nybU8c6L3Our"
# Found from error message
NAMESPACE = "surgence_lab" 
BASE_URL = "https://api.dune.com/api/v1/uploads"

HEADERS = {
    "X-DUNE-API-KEY": API_KEY,
    "Content-Type": "application/json"
}

def create_table(table_name, schema, description):
    """Creates the table in Dune."""
    url = BASE_URL
    payload = {
        "namespace": NAMESPACE,
        "table_name": table_name,
        "description": description,
        "is_private": False,
        "schema": schema
    }
    
    print(f"Creating table {NAMESPACE}.{table_name}...")
    try:
        response = requests.post(url, json=payload, headers=HEADERS)
        if response.status_code in [200, 201]:
            print("  ✅ Table created successfully")
            return True
        elif response.status_code == 409: # Conflict/Already exists
            print("  ⚠️ Table already exists (continuing)")
            return True
        else:
            print(f"  ❌ Failed to create table: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Exception creating table: {e}")
        return False

def insert_data(table_name, data_rows):
    """Inserts data using NDJSON."""
    url = f"{BASE_URL}/{NAMESPACE}/{table_name}/insert"
    
    # NDJSON headers
    ndjson_headers = HEADERS.copy()
    ndjson_headers["Content-Type"] = "application/x-ndjson"
    
    # Convert list of dicts to NDJSON string
    ndjson_data = "\n".join([json.dumps(row) for row in data_rows])
    
    print(f"  📤 Uploading {len(data_rows)} rows to {table_name}...")
    try:
        response = requests.post(url, data=ndjson_data, headers=ndjson_headers)
        if response.status_code == 200:
            print("    ✅ Data inserted successfully")
            return True
        else:
            print(f"    ❌ Insert failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"    ❌ Exception inserting data: {e}")
        return False

def process_token_transfers():
    table_name = "incentiv_token_transfers"
    file_path = "incentiv_token_transfers_backup.csv"
    
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    # Define Schema
    # Mapping CSV columns: tx_hash,block_number,timestamp,token_symbol,token_address,from_address,to_address,amount_raw,amount_decimal,is_mint,is_burn
    schema = [
        {"name": "tx_hash", "type": "varchar"},
        {"name": "block_number", "type": "double"}, # integer can overflow if not careful, double is safe for Dune uploads often, or u256
        {"name": "timestamp", "type": "timestamp"},
        {"name": "token_symbol", "type": "varchar"},
        {"name": "token_address", "type": "varchar"},
        {"name": "from_address", "type": "varchar"},
        {"name": "to_address", "type": "varchar"},
        {"name": "amount_raw", "type": "varchar"}, # Keep precision
        {"name": "amount_decimal", "type": "double"},
        {"name": "is_mint", "type": "varchar"},
        {"name": "is_burn", "type": "varchar"}
    ]

    if not create_table(table_name, schema, "Incentiv Token Transfers"):
        return

    # Read and Upload in Chunks
    CHUNK_SIZE = 5000 # NDJSON can handle larger chunks usually
    rows = []
    
    print(f"Reading {file_path}...")
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Transform types
            try:
                processed_row = {
                    "tx_hash": row["tx_hash"],
                    "block_number": int(row["block_number"]),
                    "timestamp": row["timestamp"], # ISO8601 string works for timestamp
                    "token_symbol": row["token_symbol"],
                    "token_address": row["token_address"],
                    "from_address": row["from_address"],
                    "to_address": row["to_address"],
                    "amount_raw": row["amount_raw"],
                    "amount_decimal": float(row["amount_decimal"] or 0),
                    "is_mint": str(row["is_mint"]).lower(),
                    "is_burn": str(row["is_burn"]).lower()
                }
                rows.append(processed_row)
            except ValueError as e:
                print(f"Skipping malformed row: {row} ({e})")

    total_rows = len(rows)
    print(f"Total rows to upload: {total_rows}")

    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        insert_data(table_name, chunk)
        time.sleep(1) # Rate limiting

def process_bridge_transfers():
    table_name = "incentiv_bridge_transfers"
    file_path = "incentiv_bridge_transfers_backup.csv"
    
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    # Define Schema
    # tx_hash,block_number,timestamp,direction,token_symbol,token_address,origin_chain_id,recipient,amount_raw,amount_decimal
    schema = [
        {"name": "tx_hash", "type": "varchar"},
        {"name": "block_number", "type": "integer"},
        {"name": "timestamp", "type": "timestamp", "nullable": True},
        {"name": "direction", "type": "varchar"},
        {"name": "token_symbol", "type": "varchar"},
        {"name": "token_address", "type": "varchar"},
        {"name": "origin_chain_id", "type": "integer"},
        {"name": "recipient", "type": "varchar"},
        {"name": "amount_raw", "type": "varchar"},
        {"name": "amount_decimal", "type": "double"}
    ]

    if not create_table(table_name, schema, "Incentiv Bridge Transfers"):
        return

    # Read and Upload
    CHUNK_SIZE = 5000
    rows = []
    
    print(f"Reading {file_path}...")
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                processed_row = {
                    "tx_hash": row["tx_hash"],
                    "block_number": int(row["block_number"]),
                    "timestamp": row["timestamp"] if row["timestamp"] else None,
                    "direction": row["direction"],
                    "token_symbol": row["token_symbol"],
                    "token_address": row["token_address"],
                    "origin_chain_id": int(row["origin_chain_id"] or 0),
                    "recipient": row["recipient"],
                    "amount_raw": row["amount_raw"],
                    "amount_decimal": float(row["amount_decimal"] or 0)
                }
                rows.append(processed_row)
            except ValueError as e:
                print(f"Skipping malformed row: {row} ({e})")

    total_rows = len(rows)
    print(f"Total rows to upload: {total_rows}")

    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        insert_data(table_name, chunk)
        time.sleep(1)

def process_active_wallets():
    table_name = "incentiv_active_wallets_v2"
    file_path = "incentiv_active_wallets_backup.csv"
    
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    # Schema
    # address,first_seen_block,first_seen_timestamp,last_seen_block,last_seen_timestamp,tx_count,token_transfer_count
    schema = [
        {"name": "address", "type": "varchar"},
        {"name": "first_seen_block", "type": "integer"},
        {"name": "first_seen_timestamp", "type": "timestamp", "nullable": True},
        {"name": "last_seen_block", "type": "integer"},
        {"name": "last_seen_timestamp", "type": "timestamp", "nullable": True},
        {"name": "tx_count", "type": "integer"},
        {"name": "token_transfer_count", "type": "integer"}
    ]

    if not create_table(table_name, schema, "Incentiv Active Wallets V2"):
        return

    CHUNK_SIZE = 5000
    rows = []
    
    print(f"Reading {file_path}...")
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                processed_row = {
                    "address": row["address"],
                    "first_seen_block": int(row["first_seen_block"]),
                    "first_seen_timestamp": row["first_seen_timestamp"] if row["first_seen_timestamp"] else None,
                    "last_seen_block": int(row["last_seen_block"]),
                    "last_seen_timestamp": row["last_seen_timestamp"] if row["last_seen_timestamp"] else None,
                    "tx_count": int(row["tx_count"]),
                    "token_transfer_count": int(row["token_transfer_count"])
                }
                rows.append(processed_row)
            except ValueError as e:
                print(f"Skipping malformed row: {row} ({e})")

    total_rows = len(rows)
    print(f"Total rows to upload: {total_rows}")

    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        insert_data(table_name, chunk)
        time.sleep(1)

if __name__ == "__main__":
    # process_bridge_transfers()
    # process_active_wallets()
    # process_token_transfers() # Skip for now or uncomment if needed
