import csv
import io
import json
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests


class DuneLoader:
    def __init__(self, api_key: str, base_url: str, namespace: str = "surgence_lab") -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.namespace = namespace
        self.session = requests.Session()
        self.session.headers.update({
            "X-DUNE-API-KEY": api_key,
            "Accept-Encoding": "identity" 
        })

    def _map_type(self, col_name: str, dtype: Any) -> str:
        s_dtype = str(dtype).lower()
        if "datetime" in s_dtype or "timestamp" in col_name.lower():
            return "timestamp"
        if "int64" in s_dtype:
            return "integer"
        if "float" in s_dtype or "double" in s_dtype:
            return "double"
        if "bool" in s_dtype:
            return "boolean"
        return "varchar"

    def _create_table(self, table_name: str, df: pd.DataFrame, description: str) -> None:
        """Create table using the schema-based /uploads API."""
        print(f"  ✨ Creating schema-based table {table_name}...")
        
        schema = []
        for col in df.columns:
            schema.append({
                "name": col,
                "type": self._map_type(col, df[col].dtype)
            })
            
        url = f"{self.base_url}/uploads"
        payload = {
            "namespace": self.namespace,
            "table_name": table_name,
            "description": description,
            "schema": schema,
            "is_private": False
        }
        
        resp = self.session.post(url, json=payload, timeout=60)
        if resp.status_code == 409:
             print(f"  ℹ️ Table already exists (409).")
        elif resp.status_code >= 400:
             print(f"  ❌ Failed to create table: {resp.text}")
        else:
             print(f"  ✅ Table created successfully.")

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
        # Convert datetimes to ISO strings for CSV
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        if dedupe_columns:
            df = df.drop_duplicates(subset=dedupe_columns)

        results = {"status": "completed", "rows_uploaded": 0}
        
        # Insert endpoint
        url = f"{self.base_url}/table/{self.namespace}/{table_name}/insert"
        csv_content = df.to_csv(index=False)
        
        import time
        for attempt in range(5):
            try:
                response = self.session.post(
                    url, 
                    data=csv_content, 
                    headers={"Content-Type": "text/csv"},
                    timeout=120
                )
                
                # If 404/400 might mean table doesn't exist or is legacy
                if response.status_code in [404, 400]:
                    txt = response.text.lower()
                    if "not found" in txt or "csv upload" in txt or response.status_code == 404:
                        print(f"  ⚠️ Table {table_name} issue: {response.status_code}. Attempting repair/create...")
                        self._create_table(table_name, df, description)
                        time.sleep(2) # Wait for propagation
                        # Retry
                        response = self.session.post(
                            url, 
                            data=csv_content, 
                            headers={"Content-Type": "text/csv"},
                            timeout=120
                        )

                if response.status_code >= 400:
                    print(f"Dune API Error ({response.status_code}): {response.text}")
                
                response.raise_for_status()
                results["rows_uploaded"] = len(df)
                print(f"  ✅ Successfully uploaded {len(df)} rows to {table_name}")
                return results
            except Exception as e:
                print(f"  ⚠️ Dune insert attempt {attempt+1} failed: {e}")
                time.sleep(2)
        
        raise RuntimeError(f"Dune insert failed for {table_name}")
