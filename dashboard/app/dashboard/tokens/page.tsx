'use client';

import React, { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import ChartWrapper from '@/components/ChartWrapper';
import DataTable from '@/components/DataTable';
import AddressLink from '@/components/AddressLink';
import TxLink from '@/components/TxLink';
import { formatRelativeTime, formatNumber, apiUrl } from '@/lib/helpers';
import { CONTRACT_REGISTRY, TOKEN_DECIMALS } from '@/lib/contracts';

interface TokenData {
  dailyVolume: { date: string; contract_address: string; transfer_count: number; raw_volume: string }[];
  topHolders: { contract_address: string; address: string; transfer_count: number }[];
  recentTransfers: {
    contract_address: string;
    from_addr: string;
    to_addr: string;
    value: string;
    timestamp: string;
    transaction_hash: string;
  }[];
}

const TOKEN_COLORS: Record<string, string> = {
  '0x16e43840d8d79896a389a3de85ab0b0210c05685': '#4A6CF7',  // USDC
  '0x39b076b5d23f588690d480af3bf820edad31a4bb': '#059669',  // USDT
  '0xfac24134dbc4b00ee11114ecdfe6397f389203e3': '#9333EA',  // SOL
  '0xb0f0a14a50f14dc9e6476d61c00cf0375dd4eb04': '#E55A2B',  // WCENT
};

function formatTokenVal(raw: string, contractAddr: string): string {
  if (!raw || raw === '0') return '0';
  try {
    const decimals = TOKEN_DECIMALS[contractAddr.toLowerCase()] || 18;
    const n = parseFloat(raw) / Math.pow(10, decimals);
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
    if (n >= 1_000) return formatNumber(n);
    return n.toFixed(2);
  } catch {
    return '0';
  }
}

export default function TokensPage() {
  const [selectedToken, setSelectedToken] = useState<string>('all');
  const { data, isLoading, error } = useQuery<TokenData>({
    queryKey: ['tokens'],
    queryFn: async () => {
      const r = await fetch(apiUrl('/api/incentiv/tokens'));
      const json = await r.json();
      if (json.error) throw new Error(json.error);
      return json;
    },
    retry: 3,
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10000),
  });

  const chartData = useMemo(() => {
    if (!data?.dailyVolume) return [];
    const dateMap = new Map<string, Record<string, unknown>>();
    for (const r of data.dailyVolume) {
      if (!dateMap.has(r.date)) dateMap.set(r.date, { date: r.date });
      const entry = dateMap.get(r.date)!;
      const name = CONTRACT_REGISTRY[r.contract_address]?.name || 'Unknown';
      entry[name] = r.transfer_count;
    }
    return Array.from(dateMap.values());
  }, [data]);

  const filteredTransfers = useMemo(() => {
    if (!data?.recentTransfers) return [];
    if (selectedToken === 'all') return data.recentTransfers;
    return data.recentTransfers.filter((t) => t.contract_address === selectedToken);
  }, [data, selectedToken]);

  const tokenButtons = [
    { key: 'all', label: 'All' },
    { key: '0x16e43840d8d79896a389a3de85ab0b0210c05685', label: 'USDC' },
    { key: '0x39b076b5d23f588690d480af3bf820edad31a4bb', label: 'USDT' },
    { key: '0xfac24134dbc4b00ee11114ecdfe6397f389203e3', label: 'SOL' },
    { key: '0xb0f0a14a50f14dc9e6476d61c00cf0375dd4eb04', label: 'WCENT' },
  ];

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load token data.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Token Transfer Volume Chart */}
      <TuiPanel
        title="Token Transfer Volume (90d)"
        rightContent={
          <div className="flex gap-1">
            {tokenButtons.map((t) => (
              <button
                key={t.key}
                onClick={() => setSelectedToken(t.key)}
                className={`time-pill ${selectedToken === t.key ? 'active' : ''}`}
              >
                {t.label}
              </button>
            ))}
          </div>
        }
      >
        <ChartWrapper
          data={chartData}
          type="area"
          yKeys={[
            { key: 'USDC', color: '#4A6CF7', name: 'USDC' },
            { key: 'USDT', color: '#059669', name: 'USDT' },
            { key: 'SOL', color: '#9333EA', name: 'SOL' },
            { key: 'WCENT', color: '#E55A2B', name: 'WCENT' },
          ]}
          loading={isLoading}
          height={300}
          gradientId="token-vol"
        />
      </TuiPanel>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Top Token Holders */}
        <TuiPanel title="Top Addresses by Transfer Activity" noPadding>
          <DataTable
            columns={[
              { key: 'contract_address', header: 'Token', render: (r) => {
                const row = r as Record<string, unknown>;
                const name = CONTRACT_REGISTRY[(row.contract_address as string)?.toLowerCase()]?.name || 'Unknown';
                return <span className="text-accent-cyan text-xs font-semibold">{name}</span>;
              }},
              { key: 'address', header: 'Address', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.address as string} />;
              }},
              { key: 'transfer_count', header: 'Transfers', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-orange">{formatNumber(row.transfer_count as number)}</span>;
              }},
            ]}
            data={(data?.topHolders || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={10}
          />
        </TuiPanel>

        {/* Recent Transfers */}
        <TuiPanel title="Recent Token Transfers" noPadding>
          <DataTable
            columns={[
              { key: 'contract_address', header: 'Token', render: (r) => {
                const row = r as Record<string, unknown>;
                const addr = (row.contract_address as string)?.toLowerCase();
                const name = CONTRACT_REGISTRY[addr]?.name || 'Unknown';
                return <AddressLink address={row.contract_address as string} />;
              }},
              { key: 'from_addr', header: 'From', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.from_addr as string} />;
              }},
              { key: 'to_addr', header: 'To', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.to_addr as string} />;
              }},
              { key: 'value', header: 'Amount', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-foreground">{formatTokenVal(row.value as string, row.contract_address as string)}</span>;
              }},
              { key: 'timestamp', header: 'Time', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-text-muted text-xs">{formatRelativeTime(row.timestamp as string)}</span>;
              }},
              { key: 'transaction_hash', header: 'Tx', render: (r) => {
                const row = r as Record<string, unknown>;
                return <TxLink hash={row.transaction_hash as string} />;
              }},
            ]}
            data={(filteredTransfers || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={8}
          />
        </TuiPanel>
      </div>
    </div>
  );
}
