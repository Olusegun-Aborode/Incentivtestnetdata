
import os
import sys
from supabase import create_client, Client
from dotenv import load_dotenv

# Load env
load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("❌ Missing Supabase credentials in .env")
    sys.exit(1)

supabase: Client = create_client(url, key)

REQUIRED_TABLES = [
    "blocks",
    "transactions", 
    "logs",
    "decoded_events"
]

SQL_SETUP = """
-- 1. Blocks Table
CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT PRIMARY KEY,
    hash TEXT NOT NULL,
    parent_hash TEXT,
    timestamp TIMESTAMPTZ,
    gas_used BIGINT,
    gas_limit BIGINT,
    base_fee_per_gas TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    hash TEXT PRIMARY KEY,
    block_number BIGINT REFERENCES blocks(number),
    from_address TEXT,
    to_address TEXT,
    value TEXT,
    gas_price TEXT,
    gas_used BIGINT,
    input_data TEXT,
    status BIGINT,
    timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Logs Table
CREATE TABLE IF NOT EXISTS logs (
    log_index INT,
    transaction_hash TEXT REFERENCES transactions(hash),
    block_number BIGINT REFERENCES blocks(number),
    address TEXT,
    topic0 TEXT,
    topic1 TEXT,
    topic2 TEXT,
    topic3 TEXT,
    data TEXT,
    PRIMARY KEY (transaction_hash, log_index)
);

-- 4. Decoded Events Table (simplified for flexibility)
CREATE TABLE IF NOT EXISTS decoded_events (
    id SERIAL PRIMARY KEY,
    event_name TEXT,
    contract_address TEXT,
    block_number BIGINT REFERENCES blocks(number),
    transaction_hash TEXT REFERENCES transactions(hash),
    log_index INT,
    params JSONB,
    timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tx_from ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_tx_to ON transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_decoded_event_name ON decoded_events(event_name);
"""

def init_db():
    print(f"🔌 Connecting to Supabase: {url}")
    
    # Check connection by listing tables (even if empty)
    try:
        # Use a reliable public table or just check system health if possible.
        # With anon key, we can usually select from public tables if RLS allows.
        # But we can't CREATE tables with Anon key usually.
        
        # We will try to run the SQL using the RPC interface if there is a helper function, 
        # otherwise we might fail.
        
        # NOTE: Standard Supabase Anon keys CANNOT create tables. 
        # The user needs to run the SQL in the SQL Editor.
        
        print("\n" + "="*60)
        print("⚠️  IMPORTANT: Database Schema Setup")
        print("="*60)
        print("Since you are using an 'Anon Key', I cannot automatically create tables for you.")
        print("Please copy the following SQL and run it in your Supabase SQL Editor:")
        print("-" * 60)
        print(SQL_SETUP)
        print("-" * 60)
        print("\nOnce you have run this SQL, press Enter to continue verification...")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    init_db()
