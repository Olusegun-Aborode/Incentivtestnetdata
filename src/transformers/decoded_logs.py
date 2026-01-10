
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import pandera as pa
import re
from eth_abi import decode as abi_decode
from eth_utils import keccak
from pandera import Check, Column

from src.transformers.event_router import get_table_for_event, get_schema_for_table


BASE_COLUMNS = {
    "block_number": Column(int, Check.ge(0)),
    "block_timestamp": Column("datetime64[ns]"),
    "tx_hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
    "log_index": Column(int, Check.ge(0)),
    "address": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$")),
    "event_name": Column(str),
    "chain": Column(str),
    "extracted_at": Column("datetime64[ns]"),
}


def _is_dynamic_type(type_str: str) -> bool:
    if type_str in {"string", "bytes"}:
        return True
    if type_str.endswith("[]"):
        return True
    if "[" in type_str and "]" in type_str:
        return True
    return False


def _normalize_value(value: Any) -> str:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return f"0x{bytes(value).hex()}"
    if isinstance(value, (list, tuple)):
        return json.dumps([_normalize_value(item) for item in value])
    if isinstance(value, str):
        return value.lower() if value.startswith("0x") else value
    return str(value)


def _normalize_column_name(name: str, position: int) -> str:
    if not name:
        return f"arg_{position}"
    # Handle leading underscores
    name = name.lstrip('_')
    # Convert camelCase to snake_case
    normalized = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    # Remove any non-alphanumeric characters except underscores
    normalized = re.sub(r'[^a-z0-9_]+', '_', normalized).strip('_')
    if not normalized:
        return f"arg_{position}"
    return normalized


def _input_name(input_abi: Dict[str, Any], position: int) -> str:
    name = input_abi.get("name")
    return _normalize_column_name(name, position)


def _event_signature(event_abi: Dict[str, Any]) -> Tuple[str, str]:
    types = ",".join(input_abi["type"] for input_abi in event_abi.get("inputs", []))
    signature = f"{event_abi['name']}({types})"
    topic0 = f"0x{keccak(text=signature).hex()}"
    return signature, topic0.lower()


def _load_abi_entries(abi_dir: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for abi_file in sorted(abi_dir.glob("*.json")):
        content = json.loads(abi_file.read_text())
        if isinstance(content, dict) and "abi" in content:
            content = content["abi"]
        if isinstance(content, list):
            entries.extend(content)
    return entries


def _build_event_registry(abi_dir: Path) -> Dict[str, Dict[str, Any]]:
    event_registry: Dict[str, Dict[str, Any]] = {}
    for entry in _load_abi_entries(abi_dir):
        if entry.get("type") != "event":
            continue
        _, topic0 = _event_signature(entry)
        event_registry[topic0] = entry
    return event_registry


def _decode_event(event_abi: Dict[str, Any], log: Dict[str, Any]) -> Dict[str, str]:
    topics = log.get("topics", [])
    inputs = event_abi.get("inputs", [])
    indexed_inputs = [(pos, inp) for pos, inp in enumerate(inputs) if inp.get("indexed")]
    non_indexed_inputs = [(pos, inp) for pos, inp in enumerate(inputs) if not inp.get("indexed")]
    decoded: Dict[str, str] = {}

    for topic_index, (position, input_abi) in enumerate(indexed_inputs, start=1):
        column_name = _input_name(input_abi, position)
        if topic_index >= len(topics):
            decoded[column_name] = None
            continue
        topic_value = topics[topic_index]
        if _is_dynamic_type(input_abi["type"]):
            decoded[column_name] = topic_value.lower()
            continue
        topic_bytes = bytes.fromhex(topic_value[2:])
        decoded_value = abi_decode([input_abi["type"]], topic_bytes)[0]
        decoded[column_name] = _normalize_value(decoded_value)

    if non_indexed_inputs:
        data_hex = log.get("data", "0x")
        data_bytes = bytes.fromhex(data_hex[2:]) if data_hex.startswith("0x") else bytes.fromhex(data_hex)
        decoded_values = abi_decode([input_abi["type"] for _, input_abi in non_indexed_inputs], data_bytes)
        for (position, input_abi), value in zip(non_indexed_inputs, decoded_values):
            column_name = _input_name(input_abi, position)
            decoded[column_name] = _normalize_value(value)

    return decoded


def decode_logs(logs: List[Dict[str, Any]], chain: str, abi_dir: Path) -> Dict[str, pd.DataFrame]:
    """
    Decode logs and return a dictionary of DataFrames grouped by table.
    
    Returns:
        Dict mapping table names to DataFrames with decoded events
    """
    event_registry = _build_event_registry(abi_dir)
    extracted_at = datetime.utcnow()
    
    # Group rows by table
    table_rows: Dict[str, List[Dict[str, Any]]] = {}

    for log in logs:
        topics = log.get("topics", [])
        if not topics:
            continue
        topic0 = topics[0].lower()
        event_abi = event_registry.get(topic0)
        if not event_abi:
            continue
        
        event_name = event_abi["name"]
        table_name = get_table_for_event(event_name)
        if not table_name:
            continue
        
        decoded_fields = _decode_event(event_abi, log)
        row = {
            "block_number": int(log["blockNumber"], 16),
            "block_timestamp": log["block_timestamp"],
            "tx_hash": log["transactionHash"].lower(),
            "log_index": int(log["logIndex"], 16),
            "address": log["address"].lower(),
            "event_name": event_name,
            "chain": chain,
            "extracted_at": extracted_at,
        }
        row.update(decoded_fields)
        
        if table_name not in table_rows:
            table_rows[table_name] = []
        table_rows[table_name].append(row)

    # Convert to DataFrames with proper schemas
    result = {}
    for table_name, rows in table_rows.items():
        schema_columns = get_schema_for_table(table_name)
        if not schema_columns:
            continue
        
        # Create DataFrame with all schema columns, filling missing with None
        df = pd.DataFrame(rows)
        for col in schema_columns:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns to match schema
        df = df[schema_columns]
        result[table_name] = df

    return result
