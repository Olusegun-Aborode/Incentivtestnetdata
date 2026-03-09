from src.loaders.neon import NeonLoader
import pandas as pd

def generate_report():
    neon = NeonLoader()
    
    print("--- 📊 NEON DATABASE INDEXING REPORT ---\n")
    
    # 1. Row counts
    print("Core Table Record Counts:")
    counts = neon.get_table_counts()
    for table, count in counts.items():
        print(f"  - {table}: {count:,}")
    print("\n")
    
    # 2. Block ranges
    print("Block Ranges Indexed:")
    block_stats = neon.query_df("SELECT MIN(number) as min_block, MAX(number) as max_block, COUNT(DISTINCT hash) as total_unique FROM blocks")
    if not block_stats.empty:
        print(f"  - Earliest Block: {block_stats.iloc[0]['min_block']:,}")
        print(f"  - Latest Block: {block_stats.iloc[0]['max_block']:,}")
        print(f"  - Total Unique Blocks: {block_stats.iloc[0]['total_unique']:,}")
    print("\n")
    
    # 3. Transaction stats
    print("Transaction Statistics:")
    tx_stats = neon.query_df("SELECT MIN(block_number) as min_block, MAX(block_number) as max_block FROM transactions")
    if not tx_stats.empty:
        print(f"  - Indexed from Block: {tx_stats.iloc[0]['min_block']:,}")
        print(f"  - Up to Block: {tx_stats.iloc[0]['max_block']:,}")
    print("\n")
    
    # 4. State Tracking
    print("Pipeline State Tracking:")
    state = neon.query_df("SELECT * FROM extraction_state")
    for _, row in state.iterrows():
        print(f"  - Task '{row['extraction_type']}':")
        print(f"      Status: {row['status']}")
        print(f"      Last Block Processed: {row['last_block_processed']:,}")
        print(f"      Total Items Extracted: {row['total_items_processed']:,}")
        print(f"      Last Updated: {row['updated_at']}")
    
    neon.close()

if __name__ == "__main__":
    generate_report()
