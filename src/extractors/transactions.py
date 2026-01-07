from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import pandera as pa
from pandera import Check, Column


TRANSACTION_SCHEMA = pa.DataFrameSchema(
    {
        "block_number": Column(int, Check.ge(0)),
        "block_timestamp": Column("datetime64[ns]"),
        "hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "nonce": Column(int, Check.ge(0)),
        "transaction_index": Column(int, Check.ge(0)),
        "from_address": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$")),
        "to_address": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$"), nullable=True),
        "value": Column(float, Check.ge(0)),
        "gas": Column(int, Check.ge(0)),
        "gas_price": Column(float, Check.ge(0)),
        "input": Column(str, nullable=True),
        "block_hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "chain": Column(str),
        "extracted_at": Column("datetime64[ns]"),
    }
)


def normalize_transactions(
    blocks: List[Dict[str, Any]], chain: str
) -> pd.DataFrame:
    rows = []
    extracted_at = datetime.utcnow()
    for block in blocks:
        block_number = int(block["number"], 16)
        block_timestamp = datetime.utcfromtimestamp(int(block["timestamp"], 16))
        block_hash = block["hash"]
        
        for tx in block.get("transactions", []):
            if isinstance(tx, str):
                continue # Skip if transactions are just hashes
                
            rows.append(
                {
                    "block_number": block_number,
                    "block_timestamp": block_timestamp,
                    "hash": tx["hash"],
                    "nonce": int(tx.get("nonce", "0x0"), 16),
                    "transaction_index": int(tx.get("transactionIndex", "0x0"), 16),
                    "from_address": tx["from"].lower() if tx.get("from") else None,
                    "to_address": tx["to"].lower() if tx.get("to") else None,
                    "value": float(int(tx.get("value", "0x0"), 16)),
                    "gas": int(tx.get("gas", "0x0"), 16),
                    "gas_price": float(int(tx.get("gasPrice", "0x0"), 16)),
                    "input": tx.get("input", "0x"),
                    "block_hash": block_hash,
                    "chain": chain,
                    "extracted_at": extracted_at,
                }
            )
            
    df = pd.DataFrame(rows)
    return TRANSACTION_SCHEMA.validate(df)
