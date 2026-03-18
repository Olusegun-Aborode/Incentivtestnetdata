#!/usr/bin/env python3
import json
import os
import sys
import time
from pathlib import Path

from eth_abi import decode as abi_decode
from eth_utils import keccak
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

from src.loaders.neon import NeonLoader

def build_registry():
    registry = {}
    try:
        entrypoint_abi = json.loads(Path("config/abis/entrypoint.json").read_text())
        for entry in entrypoint_abi:
            if entry.get("type") == "event":
                types = ",".join(inp["type"] for inp in entry.get("inputs", []))
                sig = f"{entry['name']}({types})"
                topic0 = f"0x{keccak(text=sig).hex()}".lower()
                registry[topic0] = entry
    except Exception as e:
        print(f"Failed to load EntryPoint ABI: {e}")

    events = [
        ("Transfer", "Transfer(address,address,uint256)", [
            {"name": "from", "type": "address", "indexed": True},
            {"name": "to", "type": "address", "indexed": True},
            {"name": "value", "type": "uint256", "indexed": False}
        ]),
        ("Swap", "Swap(address,uint256,uint256,uint256,uint256,address)", [
            {"name": "sender", "type": "address", "indexed": True},
            {"name": "amount0In", "type": "uint256", "indexed": False},
            {"name": "amount1In", "type": "uint256", "indexed": False},
            {"name": "amount0Out", "type": "uint256", "indexed": False},
            {"name": "amount1Out", "type": "uint256", "indexed": False},
            {"name": "to", "type": "address", "indexed": True}
        ]),
        ("Swap", "Swap(address,address,int256,int256,uint160,uint128,int24)", [
            {"name": "sender", "type": "address", "indexed": True},
            {"name": "recipient", "type": "address", "indexed": True},
            {"name": "amount0", "type": "int256", "indexed": False},
            {"name": "amount1", "type": "int256", "indexed": False},
            {"name": "sqrtPriceX96", "type": "uint160", "indexed": False},
            {"name": "liquidity", "type": "uint128", "indexed": False},
            {"name": "tick", "type": "int24", "indexed": False}
        ]),
        ("ReceivedTransferRemote", "ReceivedTransferRemote(uint32,bytes32,uint256)", [
            {"name": "origin", "type": "uint32", "indexed": True},
            {"name": "recipient", "type": "bytes32", "indexed": True},
            {"name": "amount", "type": "uint256", "indexed": False}
        ]),
        ("SentTransferRemote", "SentTransferRemote(uint32,bytes32,uint256)", [
            {"name": "destination", "type": "uint32", "indexed": True},
            {"name": "recipient", "type": "bytes32", "indexed": True},
            {"name": "amount", "type": "uint256", "indexed": False}
        ]),
        ("Process", "Process(bytes32)", [
            {"name": "messageId", "type": "bytes32", "indexed": True}
        ])
    ]

    for name, sig, inputs in events:
        registry[f"0x{keccak(text=sig).hex()}".lower()] = {
            "name": name,
            "inputs": inputs
        }
    return registry


def normalize_value(value):
    if value is None: return None
    if isinstance(value, bool): return str(value).lower()
    if isinstance(value, int): return str(value)
    if isinstance(value, (bytes, bytearray)): return f"0x{bytes(value).hex()}"
    if isinstance(value, str): return value.lower() if value.startswith("0x") else value
    return str(value)


def decode_log(abi, topic0, topic1, topic2, topic3, data):
    inputs = abi.get("inputs", [])
    indexed = [(pos, i) for pos, i in enumerate(inputs) if i.get("indexed")]
    non_indexed = [(pos, i) for pos, i in enumerate(inputs) if not i.get("indexed")]

    topics = [t for t in [topic0, topic1, topic2, topic3] if t]
    params = {}

    for t_idx, (pos, inp) in enumerate(indexed, start=1):
        if t_idx < len(topics):
            topic_val = topics[t_idx]
            col_name = inp.get("name", f"arg_{pos}")
            try:
                topic_bytes = bytes.fromhex(topic_val[2:] if topic_val.startswith("0x") else topic_val)
                val = abi_decode([inp["type"]], topic_bytes)[0]
                params[col_name] = normalize_value(val)
            except:
                params[col_name] = topic_val

    if non_indexed and data and data != "0x" and len(data) > 2:
        try:
            data_bytes = bytes.fromhex(data[2:] if data.startswith("0x") else data)
            types = [i["type"] for _, i in non_indexed]
            vals = abi_decode(types, data_bytes)
            for (pos, inp), val in zip(non_indexed, vals):
                params[inp.get("name", f"arg_{pos}")] = normalize_value(val)
        except Exception as e:
            params["_decode_error"] = str(e)

    return {k: v for k, v in params.items() if not k.startswith("_")}


def insert_batch(neon, rows):
    if not rows: return 0
    conn = neon.conn
    cur = conn.cursor()
    values = [
        (r["event_name"], r["contract_address"], r["block_number"],
         r["transaction_hash"], r["log_index"], r["params"],
         r["timestamp"], r["chain"]) for r in rows
    ]
    sql = """
        INSERT INTO decoded_events (event_name, contract_address, block_number, transaction_hash, log_index, params, timestamp, chain)
        VALUES %s
        ON CONFLICT (transaction_hash, log_index) DO NOTHING
    """
    try:
        psycopg2.extras.execute_values(cur, sql, values, page_size=5000)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"DB Error: {e}")
        return 0
    finally:
        cur.close()


def main():
    print("=" * 60)
    print("FAST EVENT DECODER (Transfers, Swaps, UserOps, Bridges)")
    print("=" * 60)

    registry = build_registry()
    topics = ",".join([f"'{t}'" for t in registry.keys()])
    
    neon = NeonLoader()

    print("Fetching distinct block ranges containing decodable logs...")
    # Fetch blocks with raw logs matching our topics
    block_query = f"""
        SELECT MIN(block_number), MAX(block_number) FROM raw_logs WHERE topic0 IN ({topics})
    """
    try:
        min_blk, max_blk = neon.query(block_query)[0]
    except Exception as e:
        print(e)
        return
        
    print(f"Log block range: {min_blk} to {max_blk}")
    
    chunk_size = 50000
    total_inserted = 0
    start = time.time()
    
    for chunk_start in range(min_blk, max_blk + 1, chunk_size):
        chunk_end = chunk_start + chunk_size - 1
        
        if neon.conn.closed:
            neon = NeonLoader()
            
        try:
            batch = neon.query(f"""
                SELECT id, block_number, block_timestamp, transaction_hash, log_index,
                       address, topic0, topic1, topic2, topic3, data
                FROM raw_logs
                WHERE block_number >= {chunk_start} AND block_number <= {chunk_end} 
                  AND topic0 IN ({topics})
            """)
        except Exception as e:
            print(f"Failed to fetch chunk {chunk_start}-{chunk_end}: {e}")
            neon.close()
            neon = NeonLoader()
            continue
            
        if not batch:
            continue
            
        decoded_rows = []
        for row in batch:
            _, blk_num, blk_ts, tx_hash, log_idx, addr, t0, t1, t2, t3, data = row
            abi = registry.get(t0)
            if not abi: continue
                
            params = decode_log(abi, t0, t1, t2, t3, data)
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
            inserted = insert_batch(neon, decoded_rows)
            total_inserted += inserted
            
        pct = (chunk_end - min_blk) / (max_blk - min_blk) * 100
        print(f"Blocks {chunk_start}-{chunk_end} ({pct:.1f}%) | Found {len(batch)} logs | Inserted {total_inserted:,} total")
        
    neon.close()
    print(f"\nDone in {time.time() - start:.1f}s. Total matched & inserted: {total_inserted:,}")

if __name__ == "__main__":
    main()
