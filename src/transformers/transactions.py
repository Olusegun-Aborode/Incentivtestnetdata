from datetime import datetime
from typing import Any, Dict, List, Optional

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
        "value": Column(str), # Keep as string for Dune large numbers
        "gas": Column(int, Check.ge(0)),
        "gas_price": Column(str), # Keep as string
        "input": Column(str, nullable=True),
        "status": Column(int, nullable=True), # From receipt
        "block_hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "chain": Column(str),
        "extracted_at": Column("datetime64[ns]"),
    }
)


def normalize_transactions(
    blocks: List[Dict[str, Any]], chain: str, receipts_by_hash: Optional[Dict[str, Dict[str, Any]]] = None
) -> pd.DataFrame:
    rows = []
    extracted_at = datetime.utcnow()
    receipts = receipts_by_hash or {}
    
    for block in blocks:
        block_number = int(block["number"], 16)
        block_timestamp = datetime.utcfromtimestamp(int(block["timestamp"], 16))
        block_hash = block["hash"]
        
        for tx in block.get("transactions", []):
            if isinstance(tx, str):
                continue
            
            tx_hash = tx["hash"].lower()
            receipt = receipts.get(tx_hash)
            # 0x1 usually means success
            status = int(receipt["status"], 16) if receipt and "status" in receipt else None
            
            rows.append(
                {
                    "block_number": block_number,
                    "block_timestamp": block_timestamp,
                    "hash": tx["hash"],
                    "nonce": int(tx.get("nonce", "0x0"), 16),
                    "transaction_index": int(tx.get("transactionIndex", "0x0"), 16),
                    "from_address": tx["from"].lower() if tx.get("from") else None,
                    "to_address": tx["to"].lower() if tx.get("to") else None,
                    "value": str(int(tx.get("value", "0x0"), 16)),
                    "gas": int(tx.get("gas", "0x0"), 16),
                    "gas_price": str(int(tx.get("gasPrice", "0x0"), 16)),
                    "input": tx.get("input", "0x"),
                    "status": status,
                    "block_hash": block_hash,
                    "chain": chain,
                    "extracted_at": extracted_at,
                }
            )
            
    if not rows:
        return pd.DataFrame(columns=TRANSACTION_SCHEMA.columns.keys())
        
    df = pd.DataFrame(rows)
    return TRANSACTION_SCHEMA.validate(df)
