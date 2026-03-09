from src.loaders.neon import NeonLoader

def check_state():
    neon = NeonLoader()
    print("--- NEON EXTRACTION STATE ---")
    state = neon.get_extraction_state("all_activity")
    print(f"Status: {state.get('status')}")
    print(f"Last Block Processed: {state.get('last_block_processed')}")
    print(f"Total Items: {state.get('total_items_processed')}")
    print(f"Last Updated: {state.get('updated_at')}")

    print("\n--- NEON TABLE COUNTS ---")
    counts = neon.get_table_counts()
    for table, count in counts.items():
        print(f"{table}: {count:,}")
    
    neon.close()

if __name__ == "__main__":
    check_state()
