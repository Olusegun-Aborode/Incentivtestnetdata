'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import { formatNumber, formatCompact } from '@/lib/helpers';

interface OverviewData {
  metrics: {
    totalBlocks: string;
    totalTransactions: string;
    uniqueAddresses: string;
    totalContracts: string;
    totalEvents: string;
  };
  dailyTransactions: { date: string; value: number }[];
  dailyActiveAddresses: { date: string; value: number }[];
  dailyGas: { date: string; value: number }[];
  networkStats: {
    avgBlockTime: string;
    avgTxsPerBlock: string;
    totalGas: string;
  };
}

export default function OverviewPage() {
  const { data, isLoading, error } = useQuery<OverviewData>({
    queryKey: ['overview'],
    queryFn: async () => {
      const r = await fetch('/api/incentiv/overview');
      const json = await r.json();
      if (json.error) throw new Error(json.error);
      return json;
    },
    retry: 3,
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10000),
  });

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load overview data. Please try again.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Key Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <MetricCard
          label="Total Blocks"
          value={data ? formatCompact(data.metrics.totalBlocks) : '-'}
          accent="orange"
          loading={isLoading}
        />
        <MetricCard
          label="Total Transactions"
          value={data ? formatCompact(data.metrics.totalTransactions) : '-'}
          accent="blue"
          loading={isLoading}
        />
        <MetricCard
          label="Unique Addresses"
          value={data ? formatCompact(data.metrics.uniqueAddresses) : '-'}
          accent="green"
          loading={isLoading}
        />
        <MetricCard
          label="Total Contracts"
          value={data ? formatCompact(data.metrics.totalContracts) : '-'}
          accent="purple"
          loading={isLoading}
        />
        <MetricCard
          label="Decoded Events"
          value={data ? formatCompact(data.metrics.totalEvents) : '-'}
          accent="cyan"
          loading={isLoading}
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <TuiPanel title="Daily Transactions (90d)">
          <ChartWrapper
            data={data?.dailyTransactions || []}
            type="area"
            color="#FF6B35"
            gradientId="daily-txs"
            loading={isLoading}
            height={280}
          />
        </TuiPanel>

        <TuiPanel title="Daily Active Addresses (90d)">
          <ChartWrapper
            data={data?.dailyActiveAddresses || []}
            type="area"
            color="#10B981"
            gradientId="daily-active"
            loading={isLoading}
            height={280}
          />
        </TuiPanel>
      </div>

      {/* Gas + Network Stats */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <TuiPanel title="Average Gas Usage per Block (90d)">
            <ChartWrapper
              data={data?.dailyGas || []}
              type="bar"
              color="#5B7FFF"
              loading={isLoading}
              height={250}
            />
          </TuiPanel>
        </div>

        <TuiPanel title="Network Stats (7d)">
          <div className="space-y-6">
            <div>
              <div className="metric-label">Avg Block Time</div>
              <div className="metric-value text-accent-orange mt-1">
                {isLoading ? <span className="skeleton inline-block h-6 w-16" /> : `${data?.networkStats.avgBlockTime}s`}
              </div>
            </div>
            <div>
              <div className="metric-label">Avg Txs / Block</div>
              <div className="metric-value text-accent-blue mt-1">
                {isLoading ? <span className="skeleton inline-block h-6 w-16" /> : data?.networkStats.avgTxsPerBlock}
              </div>
            </div>
            <div>
              <div className="metric-label">Total Gas Consumed</div>
              <div className="metric-value text-accent-purple mt-1">
                {isLoading ? <span className="skeleton inline-block h-6 w-24" /> : formatCompact(data?.networkStats.totalGas)}
              </div>
            </div>
          </div>
        </TuiPanel>
      </div>
    </div>
  );
}
