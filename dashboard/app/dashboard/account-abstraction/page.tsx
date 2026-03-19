'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import DataTable from '@/components/DataTable';
import AddressLink from '@/components/AddressLink';
import TxLink from '@/components/TxLink';
import { formatRelativeTime, formatNumber, formatCompact, formatEther } from '@/lib/helpers';

interface AAData {
  metrics: {
    totalOps: string;
    successRate: string;
    totalGasCost: string;
    newAccounts: string;
  };
  dailyOps: { date: string; value: number }[];
  paymasterUsage: { paymaster: string; count: number }[];
  recentOps: {
    sender: string;
    paymaster: string;
    gas_cost: string;
    success: string;
    user_op_hash: string;
    timestamp: string;
    transaction_hash: string;
  }[];
}

export default function AccountAbstractionPage() {
  const { data, isLoading, error } = useQuery<AAData>({
    queryKey: ['aa'],
    queryFn: () => fetch('/api/incentiv/aa').then((r) => r.json()),
  });

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load Account Abstraction data.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard
          label="Total UserOps"
          value={data ? formatCompact(data.metrics.totalOps) : '-'}
          accent="orange"
          loading={isLoading}
        />
        <MetricCard
          label="Success Rate"
          value={data ? `${data.metrics.successRate}%` : '-'}
          accent="green"
          loading={isLoading}
        />
        <MetricCard
          label="Total Gas Cost"
          value={data ? formatCompact(parseFloat(data.metrics.totalGasCost) / 1e18) + ' ETH' : '-'}
          accent="blue"
          loading={isLoading}
        />
        <MetricCard
          label="Accounts Deployed"
          value={data ? formatCompact(data.metrics.newAccounts) : '-'}
          accent="purple"
          loading={isLoading}
        />
      </div>

      {/* Daily UserOps Chart */}
      <TuiPanel title="Daily UserOperations (90d)">
        <ChartWrapper
          data={data?.dailyOps || []}
          type="area"
          color="#FF6B35"
          gradientId="aa-daily"
          loading={isLoading}
          height={280}
        />
      </TuiPanel>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Paymaster Usage */}
        <TuiPanel title="Paymaster Usage" noPadding>
          <DataTable
            columns={[
              { key: 'paymaster', header: 'Paymaster', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.paymaster as string} />;
              }},
              { key: 'count', header: 'UserOps Sponsored', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-orange">{formatNumber(row.count as number)}</span>;
              }},
            ]}
            data={(data?.paymasterUsage || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={8}
          />
        </TuiPanel>

        {/* Recent UserOps */}
        <TuiPanel title="Recent UserOperations" noPadding>
          <DataTable
            columns={[
              { key: 'timestamp', header: 'Time', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-text-muted text-xs">{formatRelativeTime(row.timestamp as string)}</span>;
              }},
              { key: 'sender', header: 'Sender', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.sender as string} />;
              }},
              { key: 'success', header: 'Status', render: (r) => {
                const row = r as Record<string, unknown>;
                const isSuccess = row.success === 'true' || row.success === true;
                return (
                  <span className={isSuccess ? 'badge-success' : 'badge-fail'}>
                    {isSuccess ? 'OK' : 'FAIL'}
                  </span>
                );
              }},
              { key: 'gas_cost', header: 'Gas', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                const cost = parseFloat(row.gas_cost as string || '0');
                return <span className="text-xs">{cost > 0 ? formatEther(cost) : '-'}</span>;
              }},
              { key: 'transaction_hash', header: 'Tx', render: (r) => {
                const row = r as Record<string, unknown>;
                return <TxLink hash={row.transaction_hash as string} />;
              }},
            ]}
            data={(data?.recentOps || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={10}
          />
        </TuiPanel>
      </div>
    </div>
  );
}
