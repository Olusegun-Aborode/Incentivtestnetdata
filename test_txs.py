from src.extractors.blockscout import BlockscoutExtractor
from src.config import load_yaml
from dotenv import load_dotenv

load_dotenv()
cfg = load_yaml("config/chains.yaml")["incentiv"]
e = BlockscoutExtractor(cfg["blockscout_base_url"], cfg["blockscout_rpc_url"], 10, 50, 5.0)

blocks_map = e.get_blocks_by_number([1112426, 1155000, 1155300, 1155396])
for num, b in blocks_map.items():
    print(f"Block {num} -> tx count: {len(b.get('transactions', []))}")
