export const CONTRACT_REGISTRY: Record<string, { name: string; type?: string }> = {
  '0x16e43840d8d79896a389a3de85ab0b0210c05685': { name: 'USDC', type: 'HypERC20' },
  '0x39b076b5d23f588690d480af3bf820edad31a4bb': { name: 'USDT', type: 'HypERC20' },
  '0xfac24134dbc4b00ee11114ecdfe6397f389203e3': { name: 'SOL', type: 'HypERC20' },
  '0xb0f0a14a50f14dc9e6476d61c00cf0375dd4eb04': { name: 'WCENT', type: 'Native Wrapped' },
  '0xfb7d80cf65965a66858fc0a21d611d9d63bae41e': { name: 'Veggia NFT', type: 'VGIA' },
  '0x3ec61c5633bbd7afa9144c6610930489736a72d4': { name: 'EntryPoint', type: 'ERC-4337' },
  '0xf9884c2a1749b0a02ce780ade437cbadfa3a961d': { name: 'WCENT/USDC Pool', type: 'UniswapV3' },
  '0xd1da5c73eb5b498dea4224267feea3a3de82ba4e': { name: 'WCENT/USDT Pool', type: 'UniswapV3' },
  '0x704431c81868af71a1998c36fda8a5ce6cbf10d2': { name: 'ERC1967Proxy', type: 'Proxy' },
  '0xa3999706a6407471098df0de1b94e15db2f1b64e': { name: 'MysteryBoxV2', type: 'NFT' },
  '0x79fe1f70bdc764cf4fe83c6823d81dd676c7c2a1': { name: 'EntryPoint (Legacy)', type: 'ERC-4337' },
};

export const TOKEN_DECIMALS: Record<string, number> = {
  '0x16e43840d8d79896a389a3de85ab0b0210c05685': 6,  // USDC
  '0x39b076b5d23f588690d480af3bf820edad31a4bb': 6,  // USDT
  '0xfac24134dbc4b00ee11114ecdfe6397f389203e3': 9,  // SOL
  '0xb0f0a14a50f14dc9e6476d61c00cf0375dd4eb04': 18, // WCENT
};

export const CHAIN_NAMES: Record<number, string> = {
  1: 'Ethereum',
  42161: 'Arbitrum',
  1399811149: 'Solana',
  10: 'Optimism',
  137: 'Polygon',
  8453: 'Base',
  56: 'BSC',
};

export const POOL_NAMES: Record<string, string> = {
  '0xf9884c2a1749b0a02ce780ade437cbadfa3a961d': 'WCENT/USDC',
  '0xd1da5c73eb5b498dea4224267feea3a3de82ba4e': 'WCENT/USDT',
};

export function getContractName(address: string): string | null {
  const entry = CONTRACT_REGISTRY[address.toLowerCase()];
  return entry ? entry.name : null;
}

export function getExplorerAddressUrl(address: string): string {
  return `https://explorer.incentiv.io/address/${address}`;
}

export function getExplorerTxUrl(hash: string): string {
  return `https://explorer.incentiv.io/tx/${hash}`;
}

export function getExplorerBlockUrl(number: number | string): string {
  return `https://explorer.incentiv.io/block/${number}`;
}
