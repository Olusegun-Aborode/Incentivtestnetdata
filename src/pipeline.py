import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
load_dotenv()

from src.config import load_yaml
from src.extractors.blockscout import BlockscoutExtractor
from src.handlers.dlq import DeadLetterQueue
from src.loaders.dune import DuneLoader
from src.transformers.logs import normalize_logs


def load_state(path: Path) -> Dict[str, int]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_state(path: Path, state: Dict[str, int]) -> None:
    path.write_text(json.dumps(state))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incentiv Blockscout ETL")
    parser.add_argument("--chain", default="incentiv")
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--state-file", default="state.json")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def enrich_logs_with_timestamps(logs: List[Dict], blocks: Dict[int, Dict]) -> None:
    for log in logs:
        block_number = int(log["blockNumber"], 16)
        block = blocks.get(block_number)
        if not block:
            log["block_timestamp"] = datetime.utcfromtimestamp(0)
            continue
        log["block_timestamp"] = datetime.utcfromtimestamp(int(block["timestamp"], 16))


def run_logs_etl(args: argparse.Namespace) -> None:
    chains = load_yaml("config/chains.yaml")
    events = load_yaml("config/events.yaml")
    destinations = load_yaml("config/destinations.yaml")

    chain_config = chains[args.chain]
    event_config = events[args.chain]

    extractor = BlockscoutExtractor(
        base_url=chain_config["blockscout_base_url"],
        rpc_url=chain_config["blockscout_rpc_url"],
        confirmations=int(chain_config["confirmations"]),
        batch_size=int(chain_config["batch_size"]),
        rate_limit_per_second=float(chain_config["rate_limit_per_second"]),
    )

    dune_cfg = destinations["dune"]
    dune_loader = DuneLoader(api_key=dune_cfg["api_key"], base_url=dune_cfg["base_url"])
    table_name = dune_cfg["tables"]["logs"]

    state_path = Path(args.state_file)
    state = load_state(state_path)
    last_block = state.get("last_block", 0)

    safe_block = args.to_block if args.to_block is not None else extractor.get_safe_block_number()
    start_block = args.from_block if args.from_block is not None else last_block + 1

    if start_block > safe_block:
        print("No new blocks to process.")
        return

    dlq = DeadLetterQueue()

    contracts = {k: v.lower() for k, v in event_config["contracts"].items() if v}
    topics = {k: v.lower() for k, v in event_config["topics"].items() if v}

    if not topics:
        raise RuntimeError("Missing topics in config/events.yaml")

    for start in range(start_block, safe_block + 1, extractor.batch_size):
        end = min(start + extractor.batch_size - 1, safe_block)
        for contract_name, address in contracts.items():
            for topic_name, topic in topics.items():
                try:
                    logs = extractor.get_logs(address, [topic], start, end)
                    if not logs:
                        continue
                    block_numbers = [int(log["blockNumber"], 16) for log in logs]
                    blocks = extractor.get_blocks_by_number(block_numbers)
                    enrich_logs_with_timestamps(logs, blocks)
                    df = normalize_logs(logs, chain=args.chain)
                    if args.dry_run:
                        print(
                            f"{contract_name}:{topic_name} {start}-{end} -> {len(df)} logs"
                        )
                    else:
                        dune_loader.upload_dataframe(
                            table_name=table_name,
                            df=df,
                            description=f"{args.chain} logs from Blockscout",
                            dedupe_columns=["block_number", "tx_hash", "log_index"],
                        )
                except Exception as exc:
                    dlq.send(
                        record={"contract": contract_name, "topic": topic_name},
                        error=exc,
                        context={"from_block": start, "to_block": end},
                    )
        state["last_block"] = end
        save_state(state_path, state)


def main() -> None:
    args = parse_args()
    try:
        run_logs_etl(args)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
