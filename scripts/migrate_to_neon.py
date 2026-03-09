#!/usr/bin/env python3
"""
Migrate data from Supabase to Neon PostgreSQL
Exports all tables to CSV, then imports to Neon
"""

import os
import sys
import pandas as pd
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Load environment variables
load_dotenv()
load_dotenv('.env.neon')  # Also load Neon config

# Configuration
TABLES = ['blocks', 'transactions', 'decoded_events']
BACKUP_DIR = Path('migration_backup')
BACKUP_DIR.mkdir(exist_ok=True)
BATCH_SIZE = 50000

def export_from_supabase():
    """Export all tables from Supabase to CSV"""
    print("=" * 60)
    print("PHASE 1: EXPORTING FROM SUPABASE")
    print("=" * 60)
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("❌ Error: SUPABASE_URL or SUPABASE_KEY not set in .env")
        sys.exit(1)
    
    supabase = create_client(supabase_url, supabase_key)
    
    for table in TABLES:
        print(f"\n📦 Exporting {table}...")
        offset = 0
        total_exported = 0
        
        while True:
            try:
                data = supabase.table(table)\
                    .select("*")\
                    .range(offset, offset + BATCH_SIZE - 1)\
                    .execute()
                
                if not data.data:
                    break
                
                df = pd.DataFrame(data.data)
                mode = 'w' if offset == 0 else 'a'
                header = offset == 0
                
                df.to_csv(
                    BACKUP_DIR / f"{table}.csv",
                    mode=mode,
                    header=header,
                    index=False
                )
                
                total_exported += len(data.data)
                print(f"  ✅ Exported {total_exported:,} rows")
                
                if len(data.data) < BATCH_SIZE:
                    break
                    
                offset += BATCH_SIZE
                
            except Exception as e:
                print(f"  ⚠️  Error at offset {offset}: {e}")
                print(f"  Continuing from next batch...")
                offset += BATCH_SIZE
                continue
        
        print(f"✅ {table} export complete: {total_exported:,} total rows\n")

def import_to_neon():
    """Import CSV files to Neon PostgreSQL"""
    print("=" * 60)
    print("PHASE 2: IMPORTING TO NEON")
    print("=" * 60)
    
    neon_url = os.getenv("NEON_DATABASE_URL")
    
    if not neon_url:
        print("❌ Error: NEON_DATABASE_URL not set in .env.neon")
        print("   Please complete Phase 1 (Neon setup) first")
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(neon_url)
        cursor = conn.cursor()
        
        for table in TABLES:
            csv_file = BACKUP_DIR / f"{table}.csv"
            
            if not csv_file.exists():
                print(f"⚠️  Skipping {table} - CSV file not found")
                continue
            
            print(f"\n📤 Importing {table}...")
            
            try:
                with open(csv_file, 'r') as f:
                    cursor.copy_expert(
                        f"COPY {table} FROM STDIN WITH CSV HEADER",
                        f
                    )
                
                conn.commit()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"✅ {table} imported: {count:,} rows\n")
                
            except Exception as e:
                print(f"❌ Error importing {table}: {e}")
                conn.rollback()
                continue
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error connecting to Neon: {e}")
        sys.exit(1)

def verify_migration():
    """Verify data was migrated correctly"""
    print("=" * 60)
    print("PHASE 3: VERIFICATION")
    print("=" * 60)
    
    neon_url = os.getenv("NEON_DATABASE_URL")
    
    if not neon_url:
        print("❌ Error: NEON_DATABASE_URL not set")
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(neon_url)
        cursor = conn.cursor()
        
        print("\n📊 Table Statistics:\n")
        
        for table in TABLES:
            if table == 'blocks':
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        MIN(number) as min_block,
                        MAX(number) as max_block,
                        COUNT(CASE WHEN timestamp IS NOT NULL THEN 1 END) as with_metadata
                    FROM blocks
                """)
            elif table == 'transactions':
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        MIN(block_number) as min_block,
                        MAX(block_number) as max_block,
                        COUNT(CASE WHEN from_address IS NOT NULL THEN 1 END) as with_metadata
                    FROM transactions
                """)
            else:  # decoded_events
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        MIN(block_number) as min_block,
                        MAX(block_number) as max_block,
                        COUNT(*) as with_metadata
                    FROM decoded_events
                """)
            
            result = cursor.fetchone()
            print(f"{table}:")
            print(f"  Total rows: {result[0]:,}")
            print(f"  Block range: {result[1]:,} → {result[2]:,}")
            print(f"  With metadata: {result[3]:,}")
            print()
        
        cursor.close()
        conn.close()
        
        print("✅ Verification complete!")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate data from Supabase to Neon')
    parser.add_argument('--export-only', action='store_true', help='Only export from Supabase')
    parser.add_argument('--import-only', action='store_true', help='Only import to Neon')
    parser.add_argument('--verify-only', action='store_true', help='Only verify migration')
    
    args = parser.parse_args()
    
    if args.export_only:
        export_from_supabase()
    elif args.import_only:
        import_to_neon()
    elif args.verify_only:
        verify_migration()
    else:
        # Run full migration
        export_from_supabase()
        import_to_neon()
        verify_migration()
        
        print("\n" + "=" * 60)
        print("🎉 MIGRATION COMPLETE!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Update scripts to use Neon (Phase 4)")
        print("2. Test backfill with Neon (Phase 6)")
        print("3. Keep Supabase active for 7 days as backup")
