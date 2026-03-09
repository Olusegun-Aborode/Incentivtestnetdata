"""
Raw log normalizer — converts RPC log dicts into a DataFrame
matching the raw_logs Neon table schema.

Unlike decoded_logs.py which matches events to ABIs, this preserves
ALL logs in their raw form (topic0-3 + data as hex strings).
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


def normalize_raw_logs(logs: List[Dict[str, Any]], chain: str = "incentiv") -> pd.DataFrame:
    """
    Convert raw RPC log responses to a DataFrame matching raw_logs table.

    Args:
        logs: List of RPC log objects (from eth_getLogs)
        chain: Chain identifier

    Returns:
        DataFrame with columns: block_number, transaction_hash, log_index,
        address, topic0, topic1, topic2, topic3, data, block_timestamp,
        chain, extracted_at
    """
    if not logs:
        return pd.DataFrame(columns=[
            "block_number", "transaction_hash", "log_index", "address",
            "topic0", "topic1", "topic2", "topic3", "data",
            "block_timestamp", "chain", "extracted_at"
        ])

    extracted_at = datetime.utcnow()
    rows = []

    for log in logs:
        topics = log.get("topics", [])

        row = {
            "block_number": int(log["blockNumber"], 16),
            "transaction_hash": log["transactionHash"].lower(),
            "log_index": int(log["logIndex"], 16),
            "address": log.get("address", "").lower(),
            "topic0": topics[0].lower() if len(topics) > 0 else None,
            "topic1": topics[1].lower() if len(topics) > 1 else None,
            "topic2": topics[2].lower() if len(topics) > 2 else None,
            "topic3": topics[3].lower() if len(topics) > 3 else None,
            "data": log.get("data", "0x"),
            "block_timestamp": log.get("block_timestamp"),
            "chain": chain,
            "extracted_at": extracted_at,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Ensure timestamp is proper datetime
    if "block_timestamp" in df.columns:
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], errors="coerce", utc=True)

    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    return df
