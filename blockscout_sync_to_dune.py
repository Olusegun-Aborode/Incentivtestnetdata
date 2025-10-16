import os
import csv
import json
import time
from io import StringIO
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

# Config
DUNE_API_KEY = os.getenv('DUNE_API_KEY')
DUNE_TX_TABLE_NAME = os.getenv('DUNE_TX_TABLE_NAME', 'incentiv_testnet_transactions')
DUNE_BLOCK_TABLE_NAME = os.getenv('DUNE_BLOCK_TABLE_NAME', 'incentiv_testnet_blocks')
BLOCKSCOUT_BASE = os.getenv('BLOCKSCOUT_API_BASE', 'https://explorer-testnet.incentiv.io/api/v2')
MAX_HTTP_RETRIES = int(os.getenv('MAX_HTTP_RETRIES', '5'))
BACKOFF_BASE_SECONDS = float(os.getenv('BACKOFF_BASE_SECONDS', '1'))
BACKOFF_MAX_SECONDS = float(os.getenv('BACKOFF_MAX_SECONDS', '16'))
MAX_ITEMS_TRANSACTIONS = int(os.getenv('MAX_ITEMS_TRANSACTIONS', '1000'))
MAX_ITEMS_BLOCKS = int(os.getenv('MAX_ITEMS_BLOCKS', '500'))
ITEMS_COUNT_PER_PAGE = int(os.getenv('ITEMS_COUNT_PER_PAGE', '100'))  # Blockscout default page size is 50; we can request more


def http_get_json(endpoint, params=None, timeout=60):
    url = f"{BLOCKSCOUT_BASE}{endpoint}"
    last_err = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            r = requests.get(url, params=params or {}, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
            print(f"GET {endpoint} failed (attempt {attempt+1}/{MAX_HTTP_RETRIES}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"GET {endpoint} failed after {MAX_HTTP_RETRIES} attempts: {last_err}")


def upload_to_dune(table_name, rows):
    if not rows:
        print(f"No rows to upload for {table_name}.")
        return False
    if not DUNE_API_KEY:
        print('Missing DUNE_API_KEY. Set it in .env.')
        return False
    headers = {
        'X-Dune-Api-Key': DUNE_API_KEY,
        'Content-Type': 'application/json'
    }
    # Convert to CSV
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    csv_data = output.getvalue().strip()
    payload = {
        'data': csv_data,
        'description': f'Blockscout raw sync for {table_name}',
        'table_name': table_name,
        'is_private': False
    }

    last_err = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            resp = requests.post('https://api.dune.com/api/v1/table/upload/csv', headers=headers, data=json.dumps(payload), timeout=120)
            if resp.status_code == 200:
                print('Upload successful! Table:', table_name)
                return True
            else:
                last_err = f"{resp.status_code} {resp.text}"
                delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
                print(f"Upload failed (attempt {attempt+1}/{MAX_HTTP_RETRIES}): {resp.status_code}. Retrying in {delay}s...")
                time.sleep(delay)
        except requests.exceptions.RequestException as e:
            last_err = e
            delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
            print(f"Upload error (attempt {attempt+1}/{MAX_HTTP_RETRIES}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    print('Upload failed after retries:', last_err)
    return False


def map_tx_item(item):
    # Map Blockscout tx item to a stable CSV schema
    return {
        'hash': item.get('hash'),
        'block_number': item.get('block_number'),
        'timestamp': item.get('timestamp'),
        'from_address': item.get('from'),
        'to_address': item.get('to'),
        'status': item.get('status'),
        'gas_used': item.get('gas_used'),
        'gas_limit': item.get('gas_limit'),
        'gas_price': item.get('gas_price'),
        'max_fee_per_gas': item.get('max_fee_per_gas'),
        'max_priority_fee_per_gas': item.get('max_priority_fee_per_gas'),
        'base_fee_per_gas': item.get('base_fee_per_gas'),
        'priority_fee': item.get('priority_fee'),
        'fee': item.get('fee'),
        'value': item.get('value'),
        'nonce': item.get('nonce'),
        'type': item.get('type'),
        'method': item.get('method'),
        'position': item.get('position'),
        'confirmations': item.get('confirmations'),
        'created_contract': item.get('created_contract'),
        'transaction_burnt_fee': item.get('transaction_burnt_fee'),
        'transaction_tag': item.get('transaction_tag'),
        'has_error_in_internal_transactions': item.get('has_error_in_internal_transactions'),
        'decoded_input': json.dumps(item.get('decoded_input')) if item.get('decoded_input') is not None else None,
        'raw_input': item.get('raw_input'),
        'token_transfers_count': len(item.get('token_transfers') or []),
        'ingested_at': datetime.now(timezone.utc).isoformat()
    }


def map_block_item(item):
    miner = item.get('miner') or {}
    return {
        'height': item.get('height') or item.get('number') or item.get('block_number'),
        'hash': item.get('hash'),
        'timestamp': item.get('timestamp'),
        'gas_used': item.get('gas_used'),
        'gas_limit': item.get('gas_limit'),
        'base_fee_per_gas': item.get('base_fee_per_gas'),
        'burnt_fees': item.get('burnt_fees'),
        'difficulty': item.get('difficulty'),
        'miner_hash': miner.get('hash'),
        'miner_name': miner.get('ens_domain_name'),
        'internal_transactions_count': item.get('internal_transactions_count'),
        'ingested_at': datetime.now(timezone.utc).isoformat()
    }


def fetch_recent_transactions(max_items=MAX_ITEMS_TRANSACTIONS):
    items = []
    params = {'items_count': ITEMS_COUNT_PER_PAGE}
    while True:
        js = http_get_json('/transactions', params=params)
        page_items = js.get('items') or []
        items.extend(page_items)
        next_params = js.get('next_page_params')
        print(f"Fetched {len(page_items)} tx items; total {len(items)}")
        if len(items) >= max_items or not next_params:
            break
        # Use keyset pagination params directly
        params = next_params
    # Trim to max_items
    items = items[:max_items]
    return [map_tx_item(i) for i in items]


def fetch_recent_blocks(max_items=MAX_ITEMS_BLOCKS):
    items = []
    params = {'items_count': ITEMS_COUNT_PER_PAGE}
    while True:
        js = http_get_json('/blocks', params=params)
        page_items = js.get('items') or []
        items.extend(page_items)
        next_params = js.get('next_page_params')
        print(f"Fetched {len(page_items)} block items; total {len(items)}")
        if len(items) >= max_items or not next_params:
            break
        params = next_params
    items = items[:max_items]
    return [map_block_item(i) for i in items]


if __name__ == '__main__':
    print('Fetching recent Blockscout data...')
    tx_rows = fetch_recent_transactions()
    blk_rows = fetch_recent_blocks()
    print(f'Uploading {len(tx_rows)} transactions to Dune table {DUNE_TX_TABLE_NAME}...')
    ok_tx = upload_to_dune(DUNE_TX_TABLE_NAME, tx_rows)
    print(f'Uploading {len(blk_rows)} blocks to Dune table {DUNE_BLOCK_TABLE_NAME}...')
    ok_blk = upload_to_dune(DUNE_BLOCK_TABLE_NAME, blk_rows)
    if ok_tx and ok_blk:
        print('Blockscout raw sync complete.')
    else:
        print('Blockscout raw sync encountered errors.')