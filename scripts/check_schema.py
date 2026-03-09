import os
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def print_schema(table):
    print(f"--- Schema for {table} ---")
    try:
        res = supabase.table(table).select("*").limit(1).execute()
        if res.data:
            print(res.data[0].keys())
        else:
            print("Table empty or no access")
    except Exception as e:
        print(f"Error: {e}")

print_schema("blocks")
print_schema("transactions")
print_schema("decoded_events")
