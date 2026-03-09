from src.extractors.blockscout import BlockscoutExtractor
from src.extractors.full_chain import FullChainExtractor
from src.transformers.transactions import normalize_transactions
from src.loaders.neon import NeonLoader

extractor = BlockscoutExtractor(
    base_url="",
    rpc_url="https://explorer.incentiv.io/api/eth-rpc",
    confirmations=12,
    batch_size=10,
    rate_limit_per_second=10,
)

full = FullChainExtractor(extractor)
b, t = full.extract_block_range(2500000, 2500000)

df = normalize_transactions(b, "incentiv")
print("DF columns:", df.columns.tolist())
print(df)

neon = NeonLoader()
try:
    inserted = neon.copy_dataframe("transactions", df)
    print(f"Inserted to Neon: {inserted}")
except Exception as e:
    print(f"Neon error: {e}")
finally:
    neon.close()
