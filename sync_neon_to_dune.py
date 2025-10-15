#!/usr/bin/env python3
import os
import csv
from io import StringIO
import psycopg2
import requests
from dotenv import load_dotenv
from urllib.parse import unquote
import json

load_dotenv()  # Loads .env variables

# Neon connection
NEON_PASSWORD_DECODED = unquote('mequW2JuPsPTfyYXZHEIaNRSQ0ga6uXvIDUj76%2BDw3M%3D')
NEON_CONN = {
    'dbname': 'neondb',
    'user': 'goldsky_writer',
    'password': NEON_PASSWORD_DECODED,
    'host': 'ep-tiny-truth-adn8dzqb-pooler.c-2.us-east-1.aws.neon.tech',
    'port': 5432,
    'sslmode': 'require'
}

DUNE_API_KEY = os.getenv('DUNE_API_KEY')
DUNE_TABLE_NAME = 'incentiv_testnet_raw_logs'  # Table name in Dune
DUNE_UPLOAD_URL = 'https://api.dune.com/api/v1/table/upload/csv'

# Incremental sync state file
LAST_TS_FILE = os.path.join(os.path.dirname(__file__), 'last_timestamp.txt')


def read_last_ts():
    try:
        with open(LAST_TS_FILE, 'r') as f:
            raw = f.read().strip()
            return int(raw) if raw else None
    except (FileNotFoundError, ValueError):
        return None


def write_last_ts(ts_value):
    if ts_value is None:
        return
    try:
        with open(LAST_TS_FILE, 'w') as f:
            f.write(str(int(ts_value)))
    except Exception:
        pass


def fetch_new_data(last_timestamp=None, limit=10000):
    conn = None
    try:
        conn = psycopg2.connect(**NEON_CONN)
        cursor = conn.cursor()
        if last_timestamp:
            query = (
                "SELECT * FROM public.incentiv_testnet_raw_logs "
                "WHERE block_timestamp > %s ORDER BY block_timestamp ASC"
            )
            cursor.execute(query, (last_timestamp,))
        else:
            query = (
                "SELECT * FROM public.incentiv_testnet_raw_logs "
                "ORDER BY block_timestamp ASC LIMIT %s"
            )
            cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        last_ts = last_timestamp
        if data:
            last_ts = data[-1]['block_timestamp']
        return data, last_ts
    finally:
        if conn:
            conn.close()


def upload_to_dune(data):
    if not data:
        print("No new data to upload.")
        return False
    if not DUNE_API_KEY:
        print("Missing DUNE_API_KEY. Set it in .env.")
        return False

    # Convert to CSV string (header + rows)
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(data[0].keys()))
    writer.writeheader()
    writer.writerows(data)
    csv_data = output.getvalue().strip()

    # Upload using JSON payload per Dune API
    headers = {
        'X-Dune-Api-Key': DUNE_API_KEY,
        'Content-Type': 'application/json'
    }
    payload = {
        'data': csv_data,
        'description': 'Incentiv testnet raw logs synced from Neon',
        'table_name': DUNE_TABLE_NAME,
        'is_private': False
    }

    try:
        response = requests.post(DUNE_UPLOAD_URL, headers=headers, data=json.dumps(payload), timeout=120)
        if response.status_code == 200:
            print("Upload successful! Table:", DUNE_TABLE_NAME)
            return True
        else:
            print("Upload failed:", response.status_code, response.text)
            return False
    except requests.RequestException as e:
        print("Upload error:", e)
        return False


if __name__ == "__main__":
    prev_last_ts = read_last_ts()
    data, last_ts = fetch_new_data(prev_last_ts)  # Initial run; for incremental, uses stored last_ts
    upload_ok = upload_to_dune(data)
    print("Sync complete. Last timestamp:", last_ts)
    if upload_ok:
        write_last_ts(last_ts)
    else:
        exit(1)