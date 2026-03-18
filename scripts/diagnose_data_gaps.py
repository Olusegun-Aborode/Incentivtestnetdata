import os
from collections import defaultdict
from src.loaders.neon import NeonLoader
from dotenv import load_dotenv

def main():
    load_dotenv()
    load_dotenv('.env.neon')
    neon = NeonLoader()

    print("="*60)
    print("INCENTIV MAINNET DATA DIAGNOSTIC")
    print("="*60)
    print()

    # 1. Total Rows
    print("--- 1. TABLE ROW COUNTS ---")
    tables = ['blocks', 'transactions', 'raw_logs', 'decoded_events', 'contracts']
    for t in tables:
        rows = neon.query(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<15}: {rows[0][0]:>12,}")
        
    # 2. Block Coverage
    print("\\n--- 2. BLOCK COVERAGE ---")
    rows = neon.query("SELECT MIN(number), MAX(number), COUNT(*) FROM blocks")
    min_blk, max_blk, count_blk = rows[0]
    print(f"  Min Block Indexed : {min_blk:,}")
    print(f"  Max Block Indexed : {max_blk:,}")
    print(f"  Total Blocks Saved: {count_blk:,}")
    expected = (max_blk - min_blk + 1) if max_blk else 0
    missing = expected - count_blk
    print(f"  Missing in Range  : {missing:,} blocks ({(missing/expected*100) if expected else 0:.2f}%)")
    print(f"  Missing to Genesis: {min_blk:,} blocks (not yet backfilled)")
    
    # 3. Transaction Join Health
    print("\\n--- 3. TRANSACTION JOIN HEALTH (The 'Low User Count' Bug) ---")
    
    # Total distinct tx hashes in decoded_events
    rows = neon.query("SELECT COUNT(DISTINCT transaction_hash) FROM decoded_events")
    unique_event_txs = rows[0][0]
    
    # How many of those exist in the transactions table?
    rows = neon.query("""
        SELECT COUNT(DISTINCT de.transaction_hash)
        FROM decoded_events de
        JOIN transactions t ON de.transaction_hash = t.hash
    """)
    matched_txs = rows[0][0]
    
    print(f"  Unique TXs with events       : {unique_event_txs:,}")
    print(f"  Events with matching TX data : {matched_txs:,} ({(matched_txs/unique_event_txs*100) if unique_event_txs else 0:.1f}%)")
    missing_txs = unique_event_txs - matched_txs
    print(f"  MISSING TX DATA              : {missing_txs:,} transactions")
    
    print("\\n--- 4. EVENT-SPECIFIC JOIN DROP-OFF ---")
    events = ['Swap', 'Transfer', 'UserOperationEvent']
    for ev in events:
        # Total events of this type
        rows1 = neon.query(f"SELECT COUNT(*) FROM decoded_events WHERE event_name = '{ev}'")
        total = rows1[0][0]
        
        # Events that survive the JOIN with transactions
        rows2 = neon.query(f"""
            SELECT COUNT(*) 
            FROM decoded_events de 
            JOIN transactions t ON de.transaction_hash = t.hash 
            WHERE de.event_name = '{ev}'
        """)
        survived = rows2[0][0]
        
        print(f"  {ev:<18} total: {total:>9,}, survived JOIN: {survived:>9,} ({(survived/total*100) if total else 0:.1f}%)")

    neon.close()

if __name__ == '__main__':
    main()
