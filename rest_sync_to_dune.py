#!/usr/bin/env python3
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
DUNE_TABLE_NAME = os.getenv('DUNE_METRICS_TABLE_NAME', 'incentiv_testnet_metrics')
INCENTIV_API_BASE = os.getenv('INCENTIV_REST_API_BASE', 'https://explorer-testnet.incentiv.io/api')
INCENTIVE_RPC_URL = os.getenv('INCENTIVE_RPC_URL')
BLOCK_WINDOW_SIZE = int(os.getenv('BLOCK_WINDOW_SIZE', '200'))
MAX_HTTP_RETRIES = int(os.getenv('MAX_HTTP_RETRIES', '5'))
MAX_RPC_RETRIES = int(os.getenv('MAX_RPC_RETRIES', str(MAX_HTTP_RETRIES)))
BACKOFF_BASE_SECONDS = float(os.getenv('BACKOFF_BASE_SECONDS', '1'))
BACKOFF_MAX_SECONDS = float(os.getenv('BACKOFF_MAX_SECONDS', '16'))
RECEIPT_FETCH_LIMIT = int(os.getenv('RECEIPT_FETCH_LIMIT', '2000'))  # cap receipts per run to keep runtime bounded
LAST_METRICS_BLOCK_FILE = os.path.join(os.path.dirname(__file__), 'last_metrics_block.txt')


def hex_to_int(h):
    try:
        return int(h, 16) if isinstance(h, str) else int(h)
    except Exception:
        return 0


def http_get(params):
    last_err = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            resp = requests.get(INCENTIV_API_BASE, params=params, timeout=60)
            resp.raise_for_status()
            j = resp.json()
            # Etherscan-style responses often have status/message
            return j
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
            print(f"HTTP GET failed (attempt {attempt+1}/{MAX_HTTP_RETRIES}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"HTTP GET failed after {MAX_HTTP_RETRIES} attempts: {last_err}")

# Add JSON-RPC helper using INCENTIVE_RPC_URL
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


def get_latest_block():
    # Use JSON-RPC eth_blockNumber for reliability
    res = json_rpc('eth_blockNumber', [])
    return hex_to_int(res)


def get_block_by_number(block_number):
    # Use JSON-RPC eth_getBlockByNumber for full tx objects
    res = json_rpc('eth_getBlockByNumber', [hex(block_number), True])
    return res


def get_transaction_receipt(tx_hash):
    # Use JSON-RPC eth_getTransactionReceipt to derive success/gas used
    res = json_rpc('eth_getTransactionReceipt', [tx_hash])
    return res


def read_last_metrics_block():
    try:
        with open(LAST_METRICS_BLOCK_FILE, 'r') as f:
            raw = f.read().strip()
            return int(raw) if raw else None
    except (FileNotFoundError, ValueError):
        return None


def write_last_metrics_block(block_number):
    try:
        with open(LAST_METRICS_BLOCK_FILE, 'w') as f:
            f.write(str(int(block_number)))
    except Exception:
        pass


def aggregate_metrics(start_block, end_block):
    transactions = []
    # Collect transactions across blocks
    print(f"Collecting transactions from blocks {start_block} to {end_block}...")
    for bn in range(start_block, end_block + 1):
        blk = get_block_by_number(bn)
        if not blk:
            continue
        txs = blk.get('transactions', [])
        transactions.extend(txs)

    total_transactions = len(transactions)

    # Fetch receipts (bounded) to compute success and fees
    success_count = 0
    total_fee_wei = 0
    avg_gas_price_wei = 0
    gas_prices = []
    unique_from = set()
    unique_to = set()

    print(f"Fetched {total_transactions} transactions. Fetching up to {RECEIPT_FETCH_LIMIT} receipts...")
    for idx, tx in enumerate(transactions):
        if idx >= RECEIPT_FETCH_LIMIT:
            break
        tx_hash = tx.get('hash')
        gas_price_hex = tx.get('gasPrice')
        if gas_price_hex:
            gas_prices.append(hex_to_int(gas_price_hex))
        # Addresses
        if tx.get('from'):
            unique_from.add(tx['from'])
        if tx.get('to'):
            unique_to.add(tx['to'])
        # Receipt
        if tx_hash:
            rcpt = get_transaction_receipt(tx_hash)
            if rcpt:
                status_hex = rcpt.get('status')
                if status_hex and hex_to_int(status_hex) == 1:
                    success_count += 1
                gas_used_hex = rcpt.get('gasUsed')
                if gas_used_hex and gas_price_hex:
                    total_fee_wei += hex_to_int(gas_used_hex) * hex_to_int(gas_price_hex)

    avg_gas_price_wei = sum(gas_prices) / len(gas_prices) if gas_prices else 0

    success_rate = (success_count / total_transactions * 100) if total_transactions else 0
    active_users = len(unique_from.union(unique_to))

    metrics_row = {
        'window_start_block': start_block,
        'window_end_block': end_block,
        'total_transactions': total_transactions,
        'total_successful_transactions': success_count,
        'transaction_success_rate': success_rate,
        'total_unique_addresses': active_users,
        'daily_average_gas_price_wei': avg_gas_price_wei,
        'total_fee_wei': total_fee_wei,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    return [metrics_row]


def upload_to_dune(rows):
    if not rows:
        print('No metrics to upload.')
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
        'description': 'Incentiv Testnet metrics from REST API',
        'table_name': DUNE_TABLE_NAME,
        'is_private': False
    }

    last_err = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            resp = requests.post('https://api.dune.com/api/v1/table/upload/csv', headers=headers, data=json.dumps(payload), timeout=120)
            if resp.status_code == 200:
                print('Upload successful! Table:', DUNE_TABLE_NAME)
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


if __name__ == '__main__':
    latest = get_latest_block()
    last_block = read_last_metrics_block()
    if last_block is not None:
        start = max(0, last_block)
    else:
        start = max(0, latest - BLOCK_WINDOW_SIZE)
    end = latest
    print(f'Aggregating metrics from block {start} to {end} (latest {latest})')
    rows = aggregate_metrics(start, end)
    ok = upload_to_dune(rows)
    if ok:
        write_last_metrics_block(end + 1)
    print('REST metrics sync complete.')