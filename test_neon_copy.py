from src.extractors.blockscout import BlockscoutExtractor
from src.transformers.blocks import normalize_blocks
from src.config import load_yaml
from dotenv import load_dotenv

load_dotenv()
cfg = load_yaml("config/chains.yaml")["incentiv"]
e = BlockscoutExtractor(cfg["blockscout_base_url"], cfg["blockscout_rpc_url"], 10, 50, 5.0)

blocks_map = e.get_blocks_by_number([1112426])
blocks = list(blocks_map.values())
df = normalize_blocks(blocks, "incentiv")
from src.loaders.neon import NeonLoader

n = NeonLoader()
try:
    print("Testing copy...")
    n.copy_dataframe("blocks", df)
    print("Success!")
except Exception as e:
    print(e)
finally:
    n.conn.cursor().execute("DELETE FROM blocks WHERE number = 1112426")
    n.conn.commit()
    n.close()
