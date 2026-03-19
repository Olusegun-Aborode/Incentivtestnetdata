'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import DataTable from '@/components/DataTable';
import AddressLink from '@/components/AddressLink';
import TxLink from '@/components/TxLink';
import { formatRelativeTime, formatNumber, formatCompact } from '@/lib/helpers';
import { POOL_NAMES } from '@/lib/contracts';

interface DexData {
  dailySwaps: { date: string; value: number }[];
  poolActivity: { contract_address: string; swap_count: number; first_swap: string; last_swap: string }[];
  recentSwaps: {
    contract_address: string;
    sender: string;
    recipient: string;
    amount0: string;
    amount1: string;
    tick: string;
    timestamp: string;
    transaction_hash: string;
  }[];
  liquidityEvents: { event_name: string; count: number }[];
}

function getPoolName(addr: string): string {
  return POOL_NAMES[addr?.toLowerCase()] || 'Unknown Pool';
}

function getSwapDirection(amount0: string, amount1: string): string {
  const a0 = parseFloat(amount0 || '0');
  const a1 = parseFloat(amount1 || '0');
  if (a0 > 0) return 'Buy';
  if (a0 < 0) return 'Sell';
  return a1 > 0 ? 'Buy' : 'Sell';
}

export default function DexPage() {
  const { data, isLoading, error } = useQuery<DexData>({
    queryKey: ['dex'],
    queryFn: () => fetch('/api/incentiv/dex').then((r) => r.json()),
  });

  const totalSwaps = data?.poolActivity?.reduce((s, p) => s + p.swap_count, 0) || 0;
  const mintCount = data?.liquidityEvents?.find((e) => e.event_name === 'Mint')?.count || 0;
  const burnCount = data?.liquidityEvents?.find((e) => e.event_name === 'Burn')?.count || 0;

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load DEX data.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard label="Total Swaps" value={formatCompact(totalSwaps)} accent="orange" loading={isLoading} />
        <MetricCard label="Active Pools" value={data?.poolActivity?.length?.toString() || '0'} accent="blue" loading={isLoading} />
        <MetricCard label="Liquidity Adds" value={formatCompact(mintCount)} accent="green" loading={isLoading} />
        <MetricCard label="Liquidity Removes" value={formatCompact(burnCount)} accent="red" loading={isLoading} />
      </div>

      {/* Swap Volume Chart */}
      <TuiPanel title="Daily Swap Count (90d)">
        <ChartWrapper
          data={data?.dailySwaps || []}
          type="area"
          color="#FF6B35"
          gradientId="dex-swaps"
          loading={isLoading}
          height={280}
        />
      </TuiPanel>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Pool Activity */}
        <TuiPanel title="Pool Activity" noPadding>
          <DataTable
            columns={[
              { key: 'contract_address', header: 'Pool', render: (r) => {
                const row = r as Record<string, unknown>;
                const name = getPoolName(row.contract_address as string);
                return (
                  <span className="flex items-center gap-2">
                    <span className="text-accent-cyan font-semibold">{name}</span>
                  </span>
                );
              }},
              { key: 'swap_count', header: 'Swaps', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-orange">{formatNumber(row.swap_count as number)}</span>;
              }},
              { key: 'last_swap', header: 'Last Activity', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-text-muted text-xs">{formatRelativeTime(row.last_swap as string)}</span>;
              }},
            ]}
            data={(data?.poolActivity || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={5}
          />
        </TuiPanel>

        {/* Recent Swaps */}
        <TuiPanel title="Recent Swaps" noPadding>
          <DataTable
            columns={[
              { key: 'timestamp', header: 'Time', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-text-muted text-xs">{formatRelativeTime(row.timestamp as string)}</span>;
              }},
              { key: 'contract_address', header: 'Pool', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-cyan text-xs">{getPoolName(row.contract_address as string)}</span>;
              }},
              { key: 'direction', header: 'Side', render: (r) => {
                const row = r as Record<string, unknown>;
                const dir = getSwapDirection(row.amount0 as string, row.amount1 as string);
                return (
                  <span className={dir === 'Buy' ? 'text-accent-green text-xs' : 'text-accent-red text-xs'}>
                    {dir}
                  </span>
                );
              }},
              { key: 'transaction_hash', header: 'Tx', render: (r) => {
                const row = r as Record<string, unknown>;
                return <TxLink hash={row.transaction_hash as string} />;
              }},
            ]}
            data={(data?.recentSwaps || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={10}
          />
        </TuiPanel>
      </div>
    </div>
  );
}
