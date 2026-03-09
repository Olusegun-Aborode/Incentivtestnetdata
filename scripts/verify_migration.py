import os
import glob
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("❌ Missing Supabase credentials")
    exit(1)

supabase: Client = create_client(url, key)

def count_csv_rows():
    files = glob.glob("backups/decoded_logs/*.csv")
    print(f"Found {len(files)} CSV files.")
    
    total_rows = 0
    for f in files:
        try:
            # Read only header to verify it exists, then count lines
            # Actually, reading the whole file into pandas is safer to handle bad lines/headers
            # but slower. Let's stick to pandas for consistency with migration script.
            df = pd.read_csv(f)
            total_rows += len(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    return total_rows

def count_supabase_rows():
    # Supabase API has a max limit for rows returned, but we can stick to count
    # count='exact' is needed
    try:
        response = supabase.table("decoded_events").select("*", count="exact", head=True).execute()
        return response.count
    except Exception as e:
        print(f"Error querying Supabase: {e}")
        return -1

def main():
    print("🔍 Starting Data Verification...")
    
    csv_count = count_csv_rows()
    print(f"📊 Total rows in CSVs: {csv_count}")
    
    sb_count = count_supabase_rows()
    print(f"mw Total rows in Supabase: {sb_count}")
    
    if sb_count >= csv_count:
        print("✅ Migration verification passed (Supabase count >= CSV count)")
    else:
        diff = csv_count - sb_count
        print(f"⚠️ Migration warning: Supabase has {diff} fewer rows than CSVs.")

if __name__ == "__main__":
    main()
