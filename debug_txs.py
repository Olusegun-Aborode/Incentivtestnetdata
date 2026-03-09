from src.extractors.blockscout import BlockscoutExtractor
from src.extractors.full_chain import FullChainExtractor

extractor = BlockscoutExtractor(
    base_url="",
    rpc_url="https://explorer.incentiv.io/api/eth-rpc",
    confirmations=12,
    batch_size=10,
    rate_limit_per_second=10,
)

full = FullChainExtractor(extractor)
b, t = full.extract_block_range(2500000, 2500000)
print(f"Blocks: {len(b)}")
print(f"Txs: {len(t)}")
if t:
    print(t[0].keys())
