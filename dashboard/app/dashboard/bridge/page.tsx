'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import DataTable from '@/components/DataTable';
import AddressLink from '@/components/AddressLink';
import TxLink from '@/components/TxLink';
import { formatRelativeTime, formatCompact } from '@/lib/helpers';
import { CONTRACT_REGISTRY, CHAIN_NAMES, TOKEN_DECIMALS } from '@/lib/contracts';

interface BridgeData {
  metrics: {
    inbound: string;
    outbound: string;
  };
  dailyBridge: { date: string; inbound: number; outbound: number }[];
  bridgeByToken: { contract_address: string; event_name: string; count: number }[];
  recentBridge: {
    event_name: string;
    contract_address: string;
    amount: string;
    chain_id: string;
    recipient: string;
    timestamp: string;
    transaction_hash: string;
  }[];
}

function getChainName(chainId: string): string {
  const id = parseInt(chainId);
  return CHAIN_NAMES[id] || `Chain ${chainId}`;
}

function formatBridgeAmount(amount: string, contractAddr: string): string {
  if (!amount || amount === '0') return '0';
  try {
    const decimals = TOKEN_DECIMALS[contractAddr?.toLowerCase()] || 18;
    const n = parseFloat(amount) / Math.pow(10, decimals);
    return formatCompact(n);
  } catch {
    return '0';
  }
}

export default function BridgePage() {
  const { data, isLoading, error } = useQuery<BridgeData>({
    queryKey: ['bridge'],
    queryFn: () => fetch('/api/incentiv/bridge').then((r) => r.json()),
  });

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load bridge data.</div>
        </div>
      </div>
    );
  }

  const totalBridge = parseInt(data?.metrics.inbound || '0') + parseInt(data?.metrics.outbound || '0');

  return (
    <div className="space-y-6">
      {/* Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <MetricCard
          label="Total Bridge Transfers"
          value={formatCompact(totalBridge)}
          accent="orange"
          loading={isLoading}
        />
        <MetricCard
          label="Inbound (Received)"
          value={data ? formatCompact(data.metrics.inbound) : '-'}
          accent="green"
          loading={isLoading}
        />
        <MetricCard
          label="Outbound (Sent)"
          value={data ? formatCompact(data.metrics.outbound) : '-'}
          accent="purple"
          loading={isLoading}
        />
      </div>

      {/* Bridge Volume Chart */}
      <TuiPanel title="Bridge Activity (90d)">
        <ChartWrapper
          data={data?.dailyBridge || []}
          type="area"
          yKeys={[
            { key: 'inbound', color: '#10B981', name: 'Inbound' },
            { key: 'outbound', color: '#B44AFF', name: 'Outbound' },
          ]}
          gradientId="bridge-vol"
          loading={isLoading}
          height={280}
        />
      </TuiPanel>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Bridge by Token */}
        <TuiPanel title="Bridge Activity by Token" noPadding>
          <DataTable
            columns={[
              { key: 'contract_address', header: 'Token', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.contract_address as string} />;
              }},
              { key: 'event_name', header: 'Direction', render: (r) => {
                const row = r as Record<string, unknown>;
                const isInbound = row.event_name === 'ReceivedTransferRemote';
                return (
                  <span className={isInbound ? 'badge-in' : 'badge-out'}>
                    {isInbound ? 'IN' : 'OUT'}
                  </span>
                );
              }},
              { key: 'count', header: 'Count', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-orange">{formatCompact(row.count as number)}</span>;
              }},
            ]}
            data={(data?.bridgeByToken || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={8}
          />
        </TuiPanel>

        {/* Recent Bridge Transfers */}
        <TuiPanel title="Recent Bridge Transfers" noPadding>
          <DataTable
            columns={[
              { key: 'event_name', header: 'Dir', render: (r) => {
                const row = r as Record<string, unknown>;
                const isInbound = row.event_name === 'ReceivedTransferRemote';
                return (
                  <span className={isInbound ? 'badge-in' : 'badge-out'}>
                    {isInbound ? 'IN' : 'OUT'}
                  </span>
                );
              }},
              { key: 'contract_address', header: 'Token', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.contract_address as string} />;
              }},
              { key: 'amount', header: 'Amount', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span>{formatBridgeAmount(row.amount as string, row.contract_address as string)}</span>;
              }},
              { key: 'chain_id', header: 'Chain', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="chain-badge">{getChainName(row.chain_id as string)}</span>;
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
            data={(data?.recentBridge || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={10}
          />
        </TuiPanel>
      </div>
    </div>
  );
}
