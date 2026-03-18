#!/usr/bin/env python3
"""
SYNC LOGS FETCH FALLBACK

A fail-proof, synchronous script to fetch missing block logs by pulling `transactions`
from Blockscout and extracting the nested `"logs"` arrays.
"""
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / '.env.neon')

BASE_URL = "https://explorer.incentiv.io/api/v2"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36"}

def fetch_json(url, retries=5):
    """Sync fetch with exponential backoff for 429/403."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 403):
                wait = (2 ** attempt) + 2
                logger.warning(f"BLOCKED {resp.status_code} on {url}. Waiting {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"HTTP {resp.status_code} on {url}: {resp.text[:100]}")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Req Error: {e}")
            time.sleep(2 ** attempt)
    return None

def fetch_txs(block_num):
    url = f"{BASE_URL}/blocks/{block_num}/transactions"
    all_txs = []
    
    while url:
        data = fetch_json(url)
        if not data:
            break
        
        items = data.get("items", [])
        all_txs.extend(items)
        
        next_page = data.get("next_page_params")
        if not next_page:
            break
            
        # Manually assemble next page URL
        query = "&".join([f"{k}={v}" for k,v in next_page.items()])
        url = f"{BASE_URL}/blocks/{block_num}/transactions?{query}"
        time.sleep(0.1)
        
    return all_txs

def run():
    gap_file = Path("data/missing_blocks.json")
    if not gap_file.exists():
        logger.error("No missing_blocks.json found.")
        return
        
    gaps = json.loads(gap_file.read_text()).get("gaps", [])
    missing = []
    for g in gaps:
        missing.extend(range(g["start"], g["end"] + 1))
        
    if not missing:
        logger.info("Nothing to do.")
        return
        
    db_url = os.environ.get("NEON_DATABASE_URL")
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    
    logger.info(f"Targeting {len(missing)} blocks for Logs...")
    
    success = 0
    errors = 0
    start = time.time()
    
    for i, block_num in enumerate(missing):
        # Throttle
        time.sleep(0.1)
        
        txs = fetch_txs(block_num)
        
        if txs is None:
            errors += 1
            continue
            
        if not txs:
            success += 1
            continue
            
        cur = conn.cursor()
        try:
            cur.execute("SELECT timestamp FROM blocks WHERE number = %s", (block_num,))
            row = cur.fetchone()
            b_ts = row[0] if row and row[0] else None
        except Exception:
            b_ts = None
            conn.rollback()
            cur = conn.cursor()
            
        logs_inserted = 0
        for tx in txs:
            tx_hash = tx.get("hash", "")
            tx_ts = tx.get("timestamp") or b_ts
            
            for log in tx.get("logs", []):
                idx = log.get("index", 0)
                addr = log.get("address", {}).get("hash", "") if isinstance(log.get("address"), dict) else str(log.get("address", ""))
                addr = addr.lower()
                data = log.get("data", "0x")
                
                t0 = log.get("topic0")
                t1 = log.get("topic1")
                t2 = log.get("topic2")
                t3 = log.get("topic3")
                
                # Treat empty string as None
                topics = [t if t and str(t).strip() else None for t in [t0, t1, t2, t3]]
                
                try:
                    cur.execute("""
                        INSERT INTO raw_logs (transaction_hash, log_index, block_number, block_timestamp,
                                           address, data, topic0, topic1, topic2, topic3, chain, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'incentiv', NOW())
                        ON CONFLICT (transaction_hash, log_index) DO NOTHING
                    """, (tx_hash, idx, block_num, tx_ts, addr, data, topics[0], topics[1], topics[2], topics[3]))
                    logs_inserted += 1
                except Exception as e:
                    logger.error(f"Insert err tx {tx_hash} idx {idx}: {e}")
                    conn.rollback()
                    cur = conn.cursor()
                    
        conn.commit()
        success += 1
        cur.close()
        
        if (i+1) % 50 == 0:
            elapsed = time.time() - start
            rate = (i+1) / elapsed
            logger.info(f"Progress: {i+1}/{len(missing)} ({((i+1)/len(missing))*100:.1f}%) | "
                        f"{rate:.1f} blk/s | OK: {success} ERR: {errors}")
            
    logger.info(f"DONE in {time.time()-start:.1f}s. OK: {success}")
    conn.close()

if __name__ == "__main__":
    run()
