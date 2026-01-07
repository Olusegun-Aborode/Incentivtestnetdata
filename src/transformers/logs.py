from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import pandera as pa
from pandera import Check, Column


LOG_SCHEMA = pa.DataFrameSchema(
    {
        "block_number": Column(int, Check.ge(0)),
        "tx_hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "log_index": Column(int, Check.ge(0)),
        "address": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$")),
        "topic0": Column(str, nullable=True),
        "topic1": Column(str, nullable=True),
        "topic2": Column(str, nullable=True),
        "topic3": Column(str, nullable=True),
        "data": Column(str),
        "block_timestamp": Column("datetime64[ns]"),
        "chain": Column(str),
        "extracted_at": Column("datetime64[ns]"),
    }
)


def normalize_logs(logs: List[Dict[str, Any]], chain: str) -> pd.DataFrame:
    rows = []
    extracted_at = datetime.utcnow()
    for log in logs:
        topics = log.get("topics", [])
        rows.append(
            {
                "block_number": int(log["blockNumber"], 16),
                "tx_hash": log["transactionHash"].lower(),
                "log_index": int(log["logIndex"], 16),
                "address": log["address"].lower(),
                "topic0": topics[0].lower() if len(topics) > 0 else None,
                "topic1": topics[1].lower() if len(topics) > 1 else None,
                "topic2": topics[2].lower() if len(topics) > 2 else None,
                "topic3": topics[3].lower() if len(topics) > 3 else None,
                "data": log.get("data", "0x").lower(),
                "block_timestamp": log["block_timestamp"],
                "chain": chain,
                "extracted_at": extracted_at,
            }
        )
    df = pd.DataFrame(rows)
    return LOG_SCHEMA.validate(df)
