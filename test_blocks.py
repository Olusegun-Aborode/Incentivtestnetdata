from src.extractors.blockscout import BlockscoutExtractor
from src.transformers.blocks import normalize_blocks
from src.config import load_yaml

cfg = load_yaml("config/chains.yaml")["incentiv"]
e = BlockscoutExtractor(cfg["blockscout_base_url"], cfg["blockscout_rpc_url"], 10, 50, 5.0)

blocks_map = e.get_blocks_by_number([1112426])
blocks = list(blocks_map.values())
df = normalize_blocks(blocks, "incentiv")
print("COLUMNS:", df.columns.tolist())
print(df.head())
