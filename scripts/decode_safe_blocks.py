#!/usr/bin/env python3
"""
Decoding Safe Blocks (Blocks that already have their transactions matched)
Extracts logs for blocks that DO NOT have foreign key violations and decodes them.
"""

import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader
import scripts.redecode_all as dec

def get_safe_blocks(neon):
    print("Finding blocks with absolutely no missing transactions...")
    start = time.time()
    
    # Blocks with no missing TXs
    query = """
        SELECT DISTINCT r.block_number
        FROM raw_logs r
        WHERE r.block_number NOT IN (
            SELECT r2.block_number
            FROM raw_logs r2
            LEFT JOIN transactions t ON r2.transaction_hash = t.hash
            WHERE t.hash IS NULL
        )
        ORDER BY r.block_number
    """
    
    rows = neon.query(query)
    blocks = [r[0] for r in rows]
    print(f"Found {len(blocks):,} safe blocks in {time.time() - start:.1f}s.")
    return blocks

def main():
    neon = NeonLoader()
    try:
        blocks = get_safe_blocks(neon)
        if not blocks:
            print("No safe blocks found. Waiting for dummy txts to insert...")
            return
            
        print("Initializing decoder registry...")
        registry = dec.build_registry()
        topics = ",".join([f"'{t}'" for t in registry.keys()])
        
        batch_size = 50000
        total_inserted = 0
        
        # Merge consecutive blocks into ranges to make querying fast
        start_time = time.time()
        
        # We process by chunks of array to not pass huge IN () clauses
        for i in range(0, len(blocks), 10000):
            chunk = blocks[i:i+10000]
            if not chunk: break
            
            blks_in = ",".join(map(str, chunk))
            try:
                batch = neon.query(f"""
                    SELECT id, block_number, block_timestamp, transaction_hash, log_index,
                           address, topic0, topic1, topic2, topic3, data
                    FROM raw_logs
                    WHERE block_number IN ({blks_in})
                      AND topic0 IN ({topics})
                """)
                
                if not batch:
                    continue
                    
                decoded_rows = []
                for row in batch:
                    _, blk_num, blk_ts, tx_hash, log_idx, addr, t0, t1, t2, t3, data = row
                    abi = registry.get(t0)
                    if not abi: continue
                        
                    import json
                    params = dec.decode_log(abi, t0, t1, t2, t3, data)
                    decoded_rows.append({
                        "event_name": abi["name"],
                        "contract_address": addr,
                        "block_number": blk_num,
                        "transaction_hash": tx_hash,
                        "log_index": log_idx,
                        "params": json.dumps(params) if params else None,
                        "timestamp": blk_ts,
                        "chain": "incentiv"
                    })
                    
                if decoded_rows:
                    inserted = dec.insert_batch(neon, decoded_rows)
                    total_inserted += inserted
                    
                print(f"[{i+10000:,}/{len(blocks):,}] Decoded {len(batch)} logs for safe blocks. Total Inserted: {total_inserted:,}")
                
            except Exception as e:
                print(f"Failed chunk offset {i}: {e}")
                neon.close()
                neon = NeonLoader()
                
        print(f"Done processing safe blocks. Inserted {total_inserted:,} decoded events in {time.time() - start_time:.1f}s")
    except Exception as e:
        print(e)
    finally:
        neon.close()

if __name__ == "__main__":
    main()
