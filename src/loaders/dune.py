import csv
import io
import json
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests


class DuneLoader:
    def __init__(self, api_key: str, base_url: str) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        # Dune API headers - try to be broad to match various API versions
        self.session.headers.update({
            "X-DUNE-API-KEY": api_key,
            "Accept-Encoding": "identity" # Avoid gzip decoding issues
        })

    def upload_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        description: str,
        dedupe_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if df.empty:
            return {"status": "empty"}

        df = df.copy()
        # Format datetimes for Dune (ISO 8601)
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        if dedupe_columns:
            df = df.drop_duplicates(subset=dedupe_columns)

        # Smaller chunks for reliability
        chunk_size = 1000
        total_rows = len(df)
        results = {"status": "completed", "rows_uploaded": 0}

        for start in range(0, total_rows, chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            csv_content = chunk.to_csv(index=False)
            
            # Using the JSON-based /uploads/csv endpoint which is known to work in manual scripts
            url = f"{self.base_url}/uploads/csv"
            payload = {
                "data": csv_content,
                "table_name": table_name,
                "description": description or f"Incentiv {table_name} data",
                "is_private": False
            }
            
            # Local retry for Dune upload
            import time
            last_exc = None
            for attempt in range(5): # More retries for flaky connections
                try:
                    # Note: We use json=payload to send as application/json
                    response = self.session.post(url, json=payload, timeout=120)
                    
                    if response.status_code >= 400:
                        print(f"Dune API Error ({response.status_code}): {response.text}")
                        if response.status_code == 400:
                             print(f"CSV Sample: {csv_content[:200]}")
                    
                    response.raise_for_status()
                    break
                except Exception as e:
                    last_exc = e
                    # Exponential backoff
                    delay = (attempt + 1) * 2
                    time.sleep(delay)
            else:
                raise RuntimeError(f"Dune upload failed after retries: {last_exc}")
                
            results["rows_uploaded"] += len(chunk)

        return results
