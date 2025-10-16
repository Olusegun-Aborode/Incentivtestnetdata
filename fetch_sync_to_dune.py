#!/usr/bin/env python3
import os
import csv
import json
from io import StringIO
import requests
from dotenv import load_dotenv
import time

load_dotenv()

DUNE_API_KEY = os.getenv('DUNE_API_KEY')
DUNE_TABLE_NAME = os.getenv('DUNE_TABLE_NAME', 'incentiv_testnet_raw_logs_rpc')
INCENTIVE_RPC_URL = os.getenv('INCENTIVE_RPC_URL')
BLOCK_BATCH_SIZE = int(os.getenv('BLOCK_BATCH_SIZE', '100'))
MAX_RPC_RETRIES = int(os.getenv('MAX_RPC_RETRIES', '5'))
BACKOFF_BASE_SECONDS = float(os.getenv('BACKOFF_BASE_SECONDS', '1'))
BACKOFF_MAX_SECONDS = float(os.getenv('BACKOFF_MAX_SECONDS', '16'))
REORG_OVERLAP_BLOCKS = int(os.getenv('REORG_OVERLAP_BLOCKS', '5'))
DUNE_UPLOAD_RETRIES = int(os.getenv('DUNE_UPLOAD_RETRIES', '3'))
LAST_BLOCK_FILE = os.path.join(os.path.dirname(__file__), 'last_block.txt')


def read_last_block():
    try:
        with open(LAST_BLOCK_FILE, 'r') as f:
            raw = f.read().strip()
            return int(raw, 0) if raw else None
    except (FileNotFoundError, ValueError):
        return None


def write_last_block(block_number):
    if block_number is None:
        return
    try:
        with open(LAST_BLOCK_FILE, 'w') as f:
            f.write(str(block_number))
    except Exception:
        pass


def json_rpc(method, params):
    if not INCENTIVE_RPC_URL:
        raise RuntimeError('INCENTIVE_RPC_URL is not set in .env')
    headers = {'Content-Type': 'application/json'}
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': method,
        'params': params
    }
    last_err = None
    for attempt in range(MAX_RPC_RETRIES):
        try:
            r = requests.post(INCENTIVE_RPC_URL, headers=headers, data=json.dumps(payload), timeout=60)
            r.raise_for_status()
            j = r.json()
            if 'error' in j:
                raise RuntimeError(f"RPC error: {j['error']}")
            return j['result']
        except (requests.exceptions.RequestException, RuntimeError) as e:
            last_err = e
            delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
            print(f"RPC call {method} failed (attempt {attempt+1}/{MAX_RPC_RETRIES}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"RPC call failed after {MAX_RPC_RETRIES} attempts: {last_err}")


def hex_to_int(h):
    return int(h, 16) if isinstance(h, str) else h


def get_latest_block():
    res = json_rpc('eth_blockNumber', [])
    return hex_to_int(res)


def fetch_logs(from_block, to_block):
    # Filter for all logs; customize address/topics as needed
    params = [{
        'fromBlock': hex(from_block),
        'toBlock': hex(to_block)
    }]
    logs = json_rpc('eth_getLogs', params)
    # Normalize fields for Dune upload
    rows = []
    for log in logs:
        rows.append({
            'block_number': hex_to_int(log.get('blockNumber')), 
            'block_hash': log.get('blockHash'),
            'transaction_hash': log.get('transactionHash'),
            'log_index': hex_to_int(log.get('logIndex')), 
            'address': log.get('address'),
            'data': log.get('data'),
            'topics': ','.join(log.get('topics', []))
        })
    return rows


def upload_to_dune(data_rows):
    if not data_rows:
        print('No new data to upload.')
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
    writer = csv.DictWriter(output, fieldnames=list(data_rows[0].keys()))
    writer.writeheader()
    writer.writerows(data_rows)
    csv_data = output.getvalue().strip()
    payload = {
        'data': csv_data,
        'description': 'Incentiv testnet raw logs fetched via RPC',
        'table_name': DUNE_TABLE_NAME,
        'is_private': False
    }
    last_err = None
    for attempt in range(DUNE_UPLOAD_RETRIES):
        try:
            resp = requests.post('https://api.dune.com/api/v1/table/upload/csv', headers=headers, data=json.dumps(payload), timeout=120)
            if resp.status_code == 200:
                print('Upload successful! Table:', DUNE_TABLE_NAME)
                return True
            else:
                last_err = f"{resp.status_code} {resp.text}"
                delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
                print(f"Upload failed (attempt {attempt+1}/{DUNE_UPLOAD_RETRIES}): {resp.status_code}. Retrying in {delay}s...")
                time.sleep(delay)
        except requests.exceptions.RequestException as e:
            last_err = e
            delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
            print(f"Upload error (attempt {attempt+1}/{DUNE_UPLOAD_RETRIES}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    print('Upload failed after retries:', last_err)
    return False


if __name__ == '__main__':
    latest = get_latest_block()
    last = read_last_block()
    if last is not None:
        start = max(0, last - REORG_OVERLAP_BLOCKS)
    else:
        start = max(0, latest - BLOCK_BATCH_SIZE)
    end = min(latest, start + BLOCK_BATCH_SIZE - 1)
    print(f'Fetching logs from block {start} to {end} (latest {latest})')
    rows = fetch_logs(start, end)
    ok = upload_to_dune(rows)
    if ok:
        write_last_block(end + 1)
    print('Sync complete. Last block:', end)