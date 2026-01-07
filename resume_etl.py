
import csv
import sys
from incentiv_dune_etl import IncentivETL, BlockscoutAPI, DuneClient, TOKENS, DECIMALS
from dune_v2_upload import create_table, insert_data, NAMESPACE
import time

def process_and_upload(etl, table_name, data, schema_desc):
    if not data:
        print(f"⚠️ No data for {table_name}")
        return

    print(f"Processing {table_name} ({len(data)} rows)...")
    
    # Convert dataclasses to list of dicts
    from dataclasses import asdict
    rows = [asdict(item) for item in data]
    
    # Define schemas
    schemas = {
        "incentiv_bridge_transfers": [
            {"name": "tx_hash", "type": "varchar"},
            {"name": "block_number", "type": "double"},
            {"name": "timestamp", "type": "timestamp", "nullable": True},
            {"name": "token_symbol", "type": "varchar"},
            {"name": "amount_raw", "type": "varchar"},
            {"name": "amount_decimal", "type": "double"},
            {"name": "sender", "type": "varchar"},
            {"name": "receiver", "type": "varchar"},
            {"name": "direction", "type": "varchar"}
        ],
        "incentiv_dex_swaps_v2": [
            {"name": "tx_hash", "type": "varchar"},
            {"name": "block_number", "type": "double"},
            {"name": "timestamp", "type": "timestamp", "nullable": True},
            {"name": "pool_address", "type": "varchar"},
            {"name": "sender", "type": "varchar"},
            {"name": "recipient", "type": "varchar"},
            {"name": "amount0", "type": "varchar"},
            {"name": "amount1", "type": "varchar"},
            {"name": "sqrt_price_x96", "type": "varchar"},
            {"name": "liquidity", "type": "varchar"},
            {"name": "tick", "type": "double"}
        ],
        "incentiv_active_wallets_v2": [
            {"name": "address", "type": "varchar"},
            {"name": "first_seen_block", "type": "double"},
            {"name": "first_seen_timestamp", "type": "timestamp", "nullable": True},
            {"name": "last_seen_block", "type": "double"},
            {"name": "last_seen_timestamp", "type": "timestamp", "nullable": True},
            {"name": "tx_count", "type": "double"},
            {"name": "token_transfer_count", "type": "double"}
        ]
    }
    
    # Handle missing timestamps correctly (use None/NULL)
    for r in rows:
        if "timestamp" in r and not r["timestamp"]:
            r["timestamp"] = None
        if "first_seen_timestamp" in r and not r["first_seen_timestamp"]:
            r["first_seen_timestamp"] = None
        if "last_seen_timestamp" in r and not r["last_seen_timestamp"]:
            r["last_seen_timestamp"] = None
            
    # Remap table names to v2 if needed
    target_table = table_name
    if table_name == "incentiv_dex_swaps":
        target_table = "incentiv_dex_swaps_v2"
    elif table_name == "incentiv_active_wallets":
        target_table = "incentiv_active_wallets_v2"
     
    schema = schemas.get(target_table)
    if not schema:
        print(f"❌ No schema defined for {target_table}")
        return

    # Create table
    create_table(target_table, schema, schema_desc)
    
    # Insert in chunks
    CHUNK_SIZE = 5000
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        insert_data(target_table, chunk)
        time.sleep(1)

def main():
    print("🚀 Resuming ETL process...")
    
    # 1. Setup
    blockscout = BlockscoutAPI()
    # Dummy DuneClient (we use v2 upload)
    dune = DuneClient("dummy") 
    etl = IncentivETL(blockscout, dune)
    
    # Populate tokens (needed for extraction)
    print("🪙 Fetching token list...")
    token_list = blockscout.get_tokens()
    for t in token_list:
        symbol = t.get("symbol", "UNKNOWN")
        address = t.get("address_hash", "")
        decimals = int(t.get("decimals", "18") or "18")
        TOKENS[symbol] = address
        DECIMALS[symbol] = decimals

    # 2. Rehydrate wallets from Token Transfers backup
    print("\n💧 Rehydrating active wallets from backup...")
    try:
        with open("incentiv_token_transfers_backup.csv", "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Track sender and receiver
                etl._track_wallet(row.get("from_address"), int(row.get("block_number", 0)), row.get("timestamp", ""))
                etl._track_wallet(row.get("to_address"), int(row.get("block_number", 0)), row.get("timestamp", ""))
                count += 1
            print(f"  ✅ Processed {count} transfer rows")
            print(f"  ✅ Current active wallets: {len(etl.wallets)}")
    except FileNotFoundError:
        print("  ❌ Backup file not found! Wallet tracking will be incomplete.")

    # 3. Extract missing data
    print("\n⛏️ Extracting remaining data...")
    
    # Bridge Transfers
    print("  • Bridge Transfers...")
    bridge_transfers = etl.extract_bridge_transfers()
    process_and_upload(etl, "incentiv_bridge_transfers", bridge_transfers, "Incentiv Bridge Transfers")
    
    # DEX Swaps
    print("  • DEX Swaps...")
    dex_swaps = etl.extract_dex_swaps()
    process_and_upload(etl, "incentiv_dex_swaps", dex_swaps, "Incentiv DEX Swaps")
    
    # Active Wallets (Final)
    print("  • Active Wallets...")
    wallets = etl.get_wallets()
    process_and_upload(etl, "incentiv_active_wallets", wallets, "Incentiv Active Wallets")

    print("\n✨ Resume Complete!")

if __name__ == "__main__":
    main()
