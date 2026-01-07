import requests
import time
import csv
import io
import os
from dataclasses import asdict, fields
from typing import List, Dict

class DuneClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.dune.com/api/v1"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Dune-Api-Key": api_key,
            "Content-Type": "application/json"
        })
        self.chunk_size = 5000  # Rows per upload batch
    
    def _save_local_backup(self, table_name: str, data: List) -> str:
        """Save data to local CSV as backup"""
        from pathlib import Path
        
        output_dir = Path("./incentiv_data")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Simple timestamp for backup
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = output_dir / f"{table_name}_{timestamp}.csv"
        
        with open(filepath, "w", newline="") as f:
            if not data:
                return str(filepath)
            
            field_names = [field.name for field in fields(data[0])]
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for item in data:
                writer.writerow(asdict(item))
        
        print(f"  💾 Saved local backup: {filepath} ({len(data)} rows)")
        return str(filepath)
    
    def _upload_chunk(self, table_name: str, chunk_data: List, chunk_num: int, is_first: bool) -> Dict:
        """Upload a single chunk to Dune"""
        url = f"{self.base_url}/table/upload/csv"
        
        # Convert to CSV string
        output = io.StringIO()
        field_names = [f.name for f in fields(chunk_data[0])]
        writer = csv.DictWriter(output, fieldnames=field_names)
        writer.writeheader()
        for item in chunk_data:
            writer.writerow(asdict(item))
        
        csv_content = output.getvalue()
        
        files = {
            "data": (f"{table_name}.csv", csv_content, "text/csv")
        }
        
        form_data = {
            "table_name": table_name,
            "description": f"Incentiv {table_name} data",
            "is_private": "false"
        }
        
        headers = {
            "X-Dune-Api-Key": self.api_key,
            "Accept-Encoding": "identity"
        }
        
        for attempt in range(3):
            try:
                resp = requests.post(url, files=files, data=form_data, headers=headers, timeout=120)
                
                if resp.status_code == 200:
                    return {"status": "success", "chunk": chunk_num}
                else:
                    print(f"    ⚠️ Chunk {chunk_num} attempt {attempt + 1} failed: {resp.status_code}")
                    time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    ⚠️ Chunk {chunk_num} attempt {attempt + 1} error: {e}")
                time.sleep(2 ** attempt)
        
        return {"status": "failed", "chunk": chunk_num}
    
    def upload_csv(self, table_name: str, data: List, description: str = "") -> Dict:
        """Upload data as CSV with chunking and local backup"""
        if not data:
            print(f"  ⚠️ No data to upload for {table_name}")
            return {"status": "empty"}
        
        # Step 1: Save local backup first
        print(f"\n  📦 Processing {table_name} ({len(data)} rows)...")
        backup_path = self._save_local_backup(table_name, data)
        
        # Step 2: Upload in chunks
        total_chunks = (len(data) + self.chunk_size - 1) // self.chunk_size
        print(f"  📤 Uploading to Dune in {total_chunks} chunk(s)...")
        
        successful = 0
        failed = 0
        
        for i in range(0, len(data), self.chunk_size):
            chunk_num = (i // self.chunk_size) + 1
            chunk = data[i:i + self.chunk_size]
            
            # For subsequent chunks, append to table name to avoid overwrite?
            # Dune API appends by default if table exists, unless we rename?
            # Actually with /table/upload/csv it usually replaces or appends depending on setup.
            # But here we probably want to append.
            # However, the original code had naming logic for parts.
            
            if chunk_num == 1:
                upload_name = table_name
            else:
                upload_name = f"{table_name}" # Keep same name to append? Or separate tables?
                # Original code: upload_name = f"{table_name}_part{chunk_num}"
                # If we use different names, we get different tables in Dune.
                # Assuming we want one table, we should use the same name.
                # BUT the original script used `f"{table_name}_part{chunk_num}"`.
                # I will preserve the original logic to match user expectation exactly.
                upload_name = f"{table_name}_part{chunk_num}"
            
            result = self._upload_chunk(upload_name, chunk, chunk_num, is_first=(chunk_num == 1))
            
            if result["status"] == "success":
                successful += 1
                print(f"    ✅ Chunk {chunk_num}/{total_chunks} uploaded ({len(chunk)} rows)")
            else:
                failed += 1
                print(f"    ❌ Chunk {chunk_num}/{total_chunks} failed")
        
        print(f"  📊 Upload complete: {successful}/{total_chunks} chunks successful")
        
        if failed > 0:
            print(f"  ℹ️ Failed chunks can be manually uploaded from: {backup_path}")
        
        return {"successful": successful, "failed": failed, "backup": backup_path}
