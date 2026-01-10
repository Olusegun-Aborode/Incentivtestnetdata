
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from eth_utils import keccak


def load_abi(path: Path) -> List[Dict[str, Any]]:
    content = json.loads(path.read_text())
    if isinstance(content, dict) and "abi" in content:
        content = content["abi"]
    if not isinstance(content, list):
        raise ValueError(f"Unsupported ABI format in {path}")
    return content


def event_topic0(event_abi: Dict[str, Any]) -> str:
    types = ",".join(input_abi["type"] for input_abi in event_abi.get("inputs", []))
    signature = f"{event_abi['name']}({types})"
    return f"0x{keccak(text=signature).hex()}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute topic0 hashes for ABI events")
    parser.add_argument("--abi", required=True, help="Path to ABI JSON file")
    args = parser.parse_args()

    abi_path = Path(args.abi)
    if not abi_path.exists():
        print(f"Error: ABI file not found at {abi_path}")
        return

    try:
        events = [entry for entry in load_abi(abi_path) if entry.get("type") == "event"]
        if not events:
            print(f"No events found in {abi_path}")
            return

        print(f"--- Topic0 Hashes for {abi_path.name} ---")
        for event in events:
            topic0 = event_topic0(event)
            print(f"{event['name']}: {topic0}")
    except Exception as e:
        print(f"Error processing ABI: {e}")


if __name__ == "__main__":
    main()
