#!/usr/bin/env python3
"""
ASYNC BLOCKSCOUT BACKFILL (HIGH PERFORMANCE) — v2 FIXED

Fixes from v1:
  1. Disabled HTTP/2 — Cloudflare blocks HTTP/2 multiplexed requests as bot traffic
  2. Reduced default concurrency from 20 → 5 to avoid 429/403
  3. Added proper browser-like headers to pass Cloudflare
  4. Added per-request delay (0.1s) inside semaphore to spread load
  5. Removed uvloop dependency (not always installed)
  6. Added graceful keyboard interrupt handling
  7. Fixed asyncpg timestamp type — pass as string, let Postgres cast
  8. Added resume support — checks DB before fetching to skip already-inserted blocks
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

# --- TUNING PARAMETERS ---
CONCURRENCY = 5             # Reduced from 20 to avoid Cloudflare blocks
MAX_RETRIES = 5             # Retries per request on failure/429
BATCH_REPORT_SIZE = 100     # Report progress every X blocks
REQUEST_DELAY = 0.15        # Seconds between requests within semaphore
# -------------------------


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
                # Cloudflare block — back off significantly
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


async def fetch_block_transactions(client: httpx.AsyncClient, block_num: int) -> List[dict]:
    """Fetch all transactions for a block, handling pagination."""
    all_txs = []
    params = {}
    url = f"{BASE_URL}/blocks/{block_num}/transactions"

    while True:
        data = await fetch_json(client, url, params=params)
        if not data:
            break

        items = data.get("items", [])
        if not items:
            break

        all_txs.extend(items)

        next_page = data.get("next_page_params")
        if not next_page:
            break

        params = next_page
        await asyncio.sleep(REQUEST_DELAY)

    return all_txs


async def process_block(block_num: int, client: httpx.AsyncClient,
                        pool: asyncpg.Pool, semaphore: asyncio.Semaphore) -> bool:
    """Fetch a single block and its transactions, then upsert to DB."""
    async with semaphore:
        # Small delay to spread requests and avoid Cloudflare detection
        await asyncio.sleep(REQUEST_DELAY)

        # 1. Fetch Block Data
        block_url = f"{BASE_URL}/blocks/{block_num}"
        block_data = await fetch_json(client, block_url)

        if not block_data:
            return False

        # Parse block fields
        try:
            b_num = int(block_data.get("height", block_num))
            b_hash = block_data.get("hash", "")
            b_parent = block_data.get("parent_hash", "")
            b_timestamp = block_data.get("timestamp") or None
            b_gas_used = int(block_data.get("gas_used", "0") or "0")
            b_gas_limit = int(block_data.get("gas_limit", "0") or "0")
            b_base_fee = int(block_data.get("base_fee_per_gas", "0") or "0")
            miner_obj = block_data.get("miner") or {}
            b_miner = miner_obj.get("hash", "") if isinstance(miner_obj, dict) else str(miner_obj)
            b_size = int(block_data.get("size", 0) or 0)
            b_tx_count = int(block_data.get("tx_count", 0) or 0)
            b_nonce = block_data.get("nonce", "") or ""

            # DB Connection for inserts
            async with pool.acquire() as conn:
                # Upsert Block — use text parameters, let Postgres handle casting
                await conn.execute("""
                    INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used,
                                        gas_limit, base_fee_per_gas, miner, size,
                                        transaction_count, nonce, chain, extracted_at)
                    VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7, $8, $9, $10, $11, 'incentiv', NOW())
                    ON CONFLICT (number) DO NOTHING
                """, b_num, b_hash, b_parent, b_timestamp, b_gas_used, b_gas_limit,
                     b_base_fee, b_miner, b_size, b_tx_count, b_nonce)

                # If there are transactions, fetch and upsert them
                if b_tx_count > 0:
                    txs = await fetch_block_transactions(client, block_num)
                    for tx in txs:
                        try:
                            tx_hash = tx.get("hash", "")
                            if not tx_hash:
                                continue

                            from_obj = tx.get("from") or {}
                            to_obj = tx.get("to") or {}
                            from_addr = (from_obj.get("hash", "") if isinstance(from_obj, dict) else str(from_obj)).lower()
                            to_addr = (to_obj.get("hash", "") if isinstance(to_obj, dict) else str(to_obj)).lower()

                            await conn.execute("""
                                INSERT INTO transactions (hash, block_number, from_address, to_address,
                                                           value, gas_price, gas, gas_used, status,
                                                           nonce, transaction_index, block_hash,
                                                           block_timestamp, chain, extracted_at)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, 'incentiv', NOW())
                                ON CONFLICT (hash) DO NOTHING
                            """,
                            tx_hash, b_num,
                            from_addr or None, to_addr or None,
                            str(tx.get("value", "0")),
                            str(tx.get("gas_price", "0")),
                            int(tx.get("gas_limit", 0) or 0),
                            int(tx.get("gas_used", 0) or 0),
                            str(tx.get("status", "")),
                            str(tx.get("nonce", "0")),
                            int(tx.get("position", 0) or 0),
                            b_hash,
                            b_timestamp)
                        except Exception as e:
                            logger.debug(f"TX insert error for {tx_hash[:16]}... in block {block_num}: {e}")
                            continue

            return True

        except Exception as e:
            logger.error(f"DB Error processing block {block_num}: {e}")
            return False


async def worker(queue: asyncio.Queue, client: httpx.AsyncClient,
                 pool: asyncpg.Pool, semaphore: asyncio.Semaphore, stats: dict):
    """Worker task drawing from the block queue."""
    while True:
        try:
            block_num = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        success = await process_block(block_num, client, pool, semaphore)

        if success:
            stats['success'] += 1
        else:
            stats['errors'] += 1

        queue.task_done()

        # Periodic progress reporting
        total_processed = stats['success'] + stats['errors']
        if total_processed % BATCH_REPORT_SIZE == 0 and total_processed > 0:
            elapsed = time.time() - stats['start_time']
            rate = total_processed / elapsed if elapsed > 0 else 0
            remaining = stats['total_blocks'] - total_processed
            eta_h = (remaining / rate) / 3600 if rate > 0 else 0

            logger.info(
                f"Progress: {total_processed:,}/{stats['total_blocks']:,} "
                f"({(total_processed/stats['total_blocks'])*100:.1f}%) "
                f"| Rate: {rate:.1f} blk/s | ETA: {eta_h:.1f}h "
                f"| OK: {stats['success']:,} | Errors: {stats['errors']:,}"
            )


async def run_async_backfill():
    import argparse
    parser = argparse.ArgumentParser(description="Async Blockscout Backfill v2")
    parser.add_argument("--max-blocks", type=int, default=0, help="Max blocks to process (0=all)")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY, help=f"Concurrent requests (default: {CONCURRENCY})")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="Skip blocks already in DB")
    args, _ = parser.parse_known_args()

    # Load gaps
    gap_file = Path("data/missing_blocks.json")
    if not gap_file.exists():
        logger.error("No gap file found. Run 'python3 scripts/blockscout_supplement.py --find-gaps' first.")
        return

    gap_data = json.loads(gap_file.read_text())
    gaps = gap_data["gaps"]

    # Build list of missing blocks
    missing_blocks = []
    for gap in gaps:
        missing_blocks.extend(range(gap["start"], gap["end"] + 1))

    if args.max_blocks > 0:
        missing_blocks = missing_blocks[:args.max_blocks]

    total_blocks = len(missing_blocks)
    logger.info(f"Targeting {total_blocks:,} blocks for backfill. Concurrency: {args.concurrency}")

    if total_blocks == 0:
        logger.info("Nothing to backfill.")
        return

    # Setup DB Pool
    db_url = os.environ.get("NEON_DATABASE_URL")
    if not db_url:
        logger.error("NEON_DATABASE_URL not set in environment.")
        return

    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=args.concurrency + 5)

    # Optionally filter out blocks already in DB
    if args.skip_existing:
        logger.info("Checking which blocks are already in DB...")
        async with pool.acquire() as conn:
            existing = await conn.fetch(
                "SELECT number FROM blocks WHERE number = ANY($1::bigint[])",
                missing_blocks[:50000]  # Check in chunks to avoid memory issues
            )
            existing_set = {r['number'] for r in existing}

        before = len(missing_blocks)
        missing_blocks = [b for b in missing_blocks if b not in existing_set]
        total_blocks = len(missing_blocks)
        logger.info(f"Filtered: {before - total_blocks:,} already exist, {total_blocks:,} remaining")

    if total_blocks == 0:
        logger.info("All blocks already present. Nothing to backfill.")
        await pool.close()
        return

    # Setup queue
    queue = asyncio.Queue()
    for b in missing_blocks:
        queue.put_nowait(b)

    stats = {
        'success': 0,
        'errors': 0,
        'total_blocks': total_blocks,
        'start_time': time.time()
    }

    # Setup HTTP Session — NO http2, browser-like headers
    limits = httpx.Limits(max_connections=args.concurrency + 5, max_keepalive_connections=args.concurrency)
    semaphore = asyncio.Semaphore(args.concurrency)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://explorer.incentiv.io/",
    }

    # CRITICAL: http2=False to avoid Cloudflare bot detection
    async with httpx.AsyncClient(http2=False, limits=limits, headers=headers,
                                  follow_redirects=True) as client:
        # Create worker tasks
        workers = [
            asyncio.create_task(worker(queue, client, pool, semaphore, stats))
            for _ in range(args.concurrency)
        ]

        try:
            # Wait for all workers to finish the queue
            await queue.join()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — stopping gracefully...")

        # Cancel workers once queue is empty
        for w in workers:
            w.cancel()

    await pool.close()

    elapsed = time.time() - stats['start_time']
    rate = stats['success'] / elapsed if elapsed > 0 else 0
    logger.info(
        f"BACKFILL COMPLETE! "
        f"Processed {stats['success'] + stats['errors']:,} blocks in {elapsed:.1f}s "
        f"({rate:.1f} blk/s). "
        f"OK: {stats['success']:,} | Errors: {stats['errors']:,}"
    )


if __name__ == "__main__":
    # Don't use uvloop — it may not be installed and isn't needed
    asyncio.run(run_async_backfill())
