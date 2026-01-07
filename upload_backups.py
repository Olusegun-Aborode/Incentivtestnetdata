
import requests
import time
import csv
import io
import sys
from typing import List, Dict
from dataclasses import dataclass, asdict, fields
import gzip

# Configuration
DUNE_API_KEY = "3nKpTZrrziBToMPOY7z2nybU8c6L3Our"
DUNE_API_BASE = "https://api.dune.com/api/v1"

class DuneClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = DUNE_API_BASE
        self.session = requests.Session()
        self.session.headers.update({
            "X-Dune-Api-Key": api_key,
            "Content-Type": "application/json"
        })
        self.chunk_size = 1000  # Reduced chunk size
    
    def _upload_chunk(self, table_name: str, csv_content: str, chunk_num: int) -> Dict:
        """Upload a single chunk to Dune"""
        url = f"{self.base_url}/table/upload/csv"
        
        # Compress CSV content
        compressed_content = gzip.compress(csv_content.encode('utf-8'))
        
        files = {
            "data": (f"{table_name}.csv", compressed_content, "text/csv")
        }
        
        form_data = {
            "table_name": table_name,
            "description": f"Incentiv {table_name} data (Manual Backup Upload)",
            "is_private": "false"
        }
        
        headers = {
            "X-Dune-Api-Key": self.api_key,
            # Requests usually handles Content-Encoding automatically or we let server detect gzip magic bytes
            # But let's be explicit if needed. However, the error "gzip decode failed" suggests it EXPECTS gzip.
            # So sending actual gzip bytes should fix it.
        }
        
        for attempt in range(5): # Increased retries
            try:
                print(f"    ⏳ Uploading chunk {chunk_num} (Attempt {attempt+1})...")
                resp = requests.post(url, files=files, data=form_data, headers=headers, timeout=120)
                
                if resp.status_code == 200:
                    return {"status": "success", "chunk": chunk_num}
                else:
                    print(f"    ⚠️ Chunk {chunk_num} attempt {attempt + 1} failed: {resp.status_code}")
                    # Try to print content even if gzip fails (accessing .content might work better than .text)
                    try:
                        print(f"      Response: {resp.content[:200]}")
                    except:
                        pass
                    time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    ⚠️ Chunk {chunk_num} attempt {attempt + 1} error: {e}")
                time.sleep(2 ** attempt)
        
        return {"status": "failed", "chunk": chunk_num}

    def upload_csv_file(self, table_name: str, file_path: str):
        """Read CSV file and upload in chunks"""
        print(f"\n📦 Processing {table_name} from {file_path}...")
        
        rows = []
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            print("  ⚠️ Empty file")
            return

        total_chunks = (len(rows) + self.chunk_size - 1) // self.chunk_size
        print(f"  📤 Uploading {len(rows)} rows in {total_chunks} chunk(s)...")
        
        successful = 0
        failed = 0
        
        for i in range(0, len(rows), self.chunk_size):
            chunk_num = (i // self.chunk_size) + 1
            chunk = rows[i:i + self.chunk_size]
            
            # Prepare CSV content for chunk
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=chunk[0].keys())
            writer.writeheader()
            writer.writerows(chunk)
            csv_content = output.getvalue()
            
            # Chunk name logic
            if chunk_num == 1:
                upload_name = table_name
            else:
                upload_name = f"{table_name}_part{chunk_num}"
            
            result = self._upload_chunk(upload_name, csv_content, chunk_num)
            
            if result["status"] == "success":
                successful += 1
                print(f"    ✅ Chunk {chunk_num}/{total_chunks} uploaded")
            else:
                failed += 1
                print(f"    ❌ Chunk {chunk_num}/{total_chunks} failed")
        
        print(f"  📊 Upload complete: {successful}/{total_chunks} chunks successful")

def main():
    dune = DuneClient(DUNE_API_KEY)
    
    files_to_upload = [
        ("incentiv_dex_swaps_v2", "incentiv_data/incentiv_dex_swaps_v2_20260106_223750.csv"),
        ("incentiv_active_wallets_v2", "incentiv_data/incentiv_active_wallets_v2_20260106_224230.csv"),
        ("incentiv_token_transfers", "incentiv_data/incentiv_token_transfers_20260106_221535.csv")
    ]
    
    for table_name, file_path in files_to_upload:
        dune.upload_csv_file(table_name, file_path)
        time.sleep(2) # Brief pause between files

if __name__ == "__main__":
    main()
