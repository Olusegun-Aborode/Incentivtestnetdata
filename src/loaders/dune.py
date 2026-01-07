import csv
import io
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests


class DuneLoader:
    def __init__(self, api_key: str, base_url: str) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-Dune-Api-Key": api_key})

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
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        if dedupe_columns:
            df = df.drop_duplicates(subset=dedupe_columns)

        chunk_size = 2000
        total_rows = len(df)
        results = {"status": "completed", "rows_uploaded": 0}

        for start in range(0, total_rows, chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=chunk.columns.tolist())
            writer.writeheader()
            for row in chunk.to_dict(orient="records"):
                writer.writerow(row)

            files = {"data": (f"{table_name}.csv", csv_buffer.getvalue(), "text/csv")}
            payload = {
                "table_name": table_name,
                "description": description,
                "is_private": "false",
            }
            response = self.session.post(f"{self.base_url}/table/upload/csv", files=files, data=payload)
            response.raise_for_status()
            results["rows_uploaded"] += len(chunk)

        return results
