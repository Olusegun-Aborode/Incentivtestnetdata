import os
import sys
import json
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def inspect_table(table, limit=5):
    print(f"--- Inspecting {table} (Limit {limit}) ---")
    try:
        res = supabase.table(table).select("*").limit(limit).execute()
        for i, row in enumerate(res.data):
            print(f"Row {i}: {json.dumps(row, default=str)}")
    except Exception as e:
        print(f"Error: {e}")

inspect_table("blocks")
inspect_table("transactions")
