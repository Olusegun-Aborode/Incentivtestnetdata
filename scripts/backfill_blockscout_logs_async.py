#!/usr/bin/env python3
"""
ASYNC BLOCKSCOUT LOGS BACKFILL (HIGH PERFORMANCE)

Fetches the raw logs for blocks that were successfully ingested by `backfill_blockscout_async.py`.
It hits `/api/v2/blocks/{block_number}/logs` and inserts them into `raw_logs`.
"""
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed. Run: pip install httpx")
    sys.exit(1)

try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg not installed. Run: pip install asyncpg")
    sys.exit(1)

from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
dotenv_path = Path(__file__).resolve().parent.parent / '.env.neon'
load_dotenv(dotenv_path)
load_dotenv()

BASE_URL = "https://explorer.incentiv.io/api/v2"
CONCURRENCY = 5
MAX_RETRIES = 5
BATCH_REPORT_SIZE = 100
REQUEST_DELAY = 0.15

async def fetch_json(client: httpx.AsyncClient, url: str, params: dict = None,
                     retries: int = MAX_RETRIES) -> Optional[dict]:
    """Fetch JSON with exponential backoff for rate limits and Cloudflare."""
    for attempt in range(retries):
        try:
            response = await client.get(url, params=params, timeout=20.0)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait_time = (2 ** attempt) + 2
                logger.debug(f"Rate limited (429) on {url}, backing off for {wait_time}s...")
                await asyncio.sleep(wait_time)
            elif response.status_code == 403:
                wait_time = (3 ** attempt) + 5
                logger.warning(f"Cloudflare 403 on {url}, backing off {wait_time}s (attempt {attempt+1})")
                await asyncio.sleep(wait_time)
            else:
                logger.warning(f"Error {response.status_code} on {url}: {response.text[:100]}")
                await asyncio.sleep(1)
        except httpx.TimeoutException:
            logger.debug(f"Timeout on {url} (attempt {attempt+1})")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.debug(f"Request exception on {url} (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch {url} after {retries} retries.")
    return None

async def fetch_block_logs(client: httpx.AsyncClient, block_num: int) -> List[dict]:
    """Fetch all logs for a block, handling pagination."""
    all_logs = []
    params = {}
    url = f"{BASE_URL}/blocks/{block_num}/logs"

    while True:
        data = await fetch_json(client, url, params=params)
        if not data:
            break

        items = data.get("items", [])
        if not items:
            break

        all_logs.extend(items)

        next_page = data.get("next_page_params")
        if not next_page:
            break

        params = next_page
        await asyncio.sleep(REQUEST_DELAY)

    return all_logs

async def process_block_logs(block_num: int, client: httpx.AsyncClient,
                             pool: asyncpg.Pool, semaphore: asyncio.Semaphore) -> bool:
    """Fetch all block transactions and extract their nested logs to upsert to DB."""
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)

        # Re-use the paginated fetcher for the block's transactions
        txs = await fetch_block_transactions(client, block_num)
        if not txs:
            return True # Successfully processed 0 logs

        b_timestamp = None
        
        async with pool.acquire() as conn:
            block_rec = await conn.fetchrow("SELECT timestamp FROM blocks WHERE number = $1", block_num)
            if block_rec and block_rec['timestamp']:
               b_timestamp = block_rec['timestamp']

            for tx in txs:
                tx_hash = tx.get("hash", "")
                tx_timestamp = tx.get("timestamp")
                if not b_timestamp:
                    b_timestamp = tx_timestamp

                # Extract nested logs from the transaction 
                nested_logs = tx.get("logs", [])
                
                for log in nested_logs:
                    try:
                        log_idx = log.get("index", 0)
                        addr = log.get("address", {}).get("hash", "").lower() if isinstance(log.get("address"), dict) else str(log.get("address", "")).lower()
                        data = log.get("data", "0x")
                        
                        if not b_timestamp:
                            # Fallback to the log's timestamp if block timestamp misses
                            b_timestamp = log.get("timestamp")

                        topics = []
                        for t in ["topic0", "topic1", "topic2", "topic3"]:
                            val = log.get(t)
                            # Ensure null string is explicitly handled as None
                            topics.append(val if val and str(val).strip() != "" else None)
                            
                        await conn.execute("""
                            INSERT INTO raw_logs (transaction_hash, log_index, block_number, block_timestamp,
                                               address, data, topic0, topic1, topic2, topic3, chain, extracted_at)
                            VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7, $8, $9, $10, 'incentiv', NOW())
                            ON CONFLICT (transaction_hash, log_index) DO NOTHING
                        """, tx_hash, log_idx, block_num, b_timestamp, addr, data, 
                           topics[0], topics[1], topics[2], topics[3])
                    except Exception as e:
                        logger.debug(f"Log insert error for tx {tx_hash[:16]} idx {log_idx}: {e}")
                        continue
        return True

async def worker(queue: asyncio.Queue, client: httpx.AsyncClient,
                 pool: asyncpg.Pool, semaphore: asyncio.Semaphore, stats: dict):
    while True:
        try:
            block_num = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        success = await process_block_logs(block_num, client, pool, semaphore)
        if success:
            stats['success'] += 1
        else:
            stats['errors'] += 1

        queue.task_done()
        total_processed = stats['success'] + stats['errors']
        if total_processed % BATCH_REPORT_SIZE == 0 and total_processed > 0:
            elapsed = time.time() - stats['start_time']
            rate = total_processed / elapsed if elapsed > 0 else 0
            remaining = stats['total_blocks'] - total_processed
            eta_h = (remaining / rate) / 3600 if rate > 0 else 0
            logger.info(
                f"Logs Progress: {total_processed:,}/{stats['total_blocks']:,} "
                f"({(total_processed/stats['total_blocks'])*100:.1f}%) "
                f"| Rate: {rate:.1f} blk/s | ETA: {eta_h:.1f}h "
                f"| OK: {stats['success']:,} | Errors: {stats['errors']:,}"
            )

async def run_async_logs_backfill():
    import argparse
    parser = argparse.ArgumentParser(description="Async Blockscout Logs Backfill")
    parser.add_argument("--max-blocks", type=int, default=0, help="Max blocks to process (0=all)")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    args, _ = parser.parse_known_args()

    gap_file = Path("data/missing_blocks.json")
    if not gap_file.exists():
        logger.error("No gap file found.")
        return

    gap_data = json.loads(gap_file.read_text())
    gaps = gap_data.get("gaps", [])

    missing_blocks = []
    for gap in gaps:
        missing_blocks.extend(range(gap["start"], gap["end"] + 1))

    if args.max_blocks > 0:
        missing_blocks = missing_blocks[:args.max_blocks]

    total_blocks = len(missing_blocks)
    logger.info(f"Targeting {total_blocks:,} blocks for Logs backfill. Concurrency: {args.concurrency}")

    if total_blocks == 0:
        logger.info("Nothing to backfill.")
        return

    db_url = os.environ.get("NEON_DATABASE_URL")
    if not db_url:
        logger.error("NEON_DATABASE_URL not set.")
        return

    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=args.concurrency + 5)
    
    queue = asyncio.Queue()
    for b in missing_blocks:
        queue.put_nowait(b)

    stats = {
        'success': 0, 'errors': 0,
        'total_blocks': total_blocks,
        'start_time': time.time()
    }

    limits = httpx.Limits(max_connections=args.concurrency + 5, max_keepalive_connections=args.concurrency)
    semaphore = asyncio.Semaphore(args.concurrency)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://explorer.incentiv.io/",
    }

    async with httpx.AsyncClient(http2=False, limits=limits, headers=headers, follow_redirects=True) as client:
        workers = [
            asyncio.create_task(worker(queue, client, pool, semaphore, stats))
            for _ in range(args.concurrency)
        ]
        try:
            await queue.join()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — stopping gracefully...")

        for w in workers:
            w.cancel()

    await pool.close()

    elapsed = time.time() - stats['start_time']
    rate = stats['success'] / elapsed if elapsed > 0 else 0
    logger.info(
        f"LOGS BACKFILL COMPLETE! Processed {stats['success'] + stats['errors']:,} blocks in {elapsed:.1f}s "
        f"({rate:.1f} blk/s). OK: {stats['success']:,} | Errors: {stats['errors']:,}"
    )

if __name__ == "__main__":
    asyncio.run(run_async_logs_backfill())
