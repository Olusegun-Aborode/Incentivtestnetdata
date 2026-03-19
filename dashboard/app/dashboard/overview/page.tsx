'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import { formatNumber, formatCompact, formatGasUnits, apiUrl } from '@/lib/helpers';

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
    avgGasPerBlock: string;
  };
}

export default function OverviewPage() {
  const { data, isLoading, error } = useQuery<OverviewData>({
    queryKey: ['overview'],
    queryFn: async () => {
      const r = await fetch(apiUrl('/api/incentiv/overview'));
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
        <TuiPanel title="Daily Transactions" tooltip="Total number of transactions processed on the Incentiv chain each day.">
          <ChartWrapper
            data={data?.dailyTransactions || []}
            type="area"
            color="#E55A2B"
            gradientId="daily-txs"
            loading={isLoading}
            height={280}
          />
        </TuiPanel>

        <TuiPanel title="Daily Active Addresses" tooltip="Unique addresses that sent or received transactions each day (from + to addresses).">
          <ChartWrapper
            data={data?.dailyActiveAddresses || []}
            type="area"
            color="#059669"
            gradientId="daily-active"
            loading={isLoading}
            height={280}
          />
        </TuiPanel>
      </div>

      {/* Gas + Network Stats */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <TuiPanel title="Average Gas Usage per Block" tooltip="Average gas consumed per block, showing network demand over time. Higher values indicate more computational activity.">
            <ChartWrapper
              data={data?.dailyGas || []}
              type="bar"
              color="#4A6CF7"
              loading={isLoading}
              height={250}
            />
          </TuiPanel>
        </div>

        <TuiPanel title="Network Stats (7d)" tooltip="Key network performance metrics averaged over the last 7 days. CENT is the native token of Incentiv chain.">
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
              <div className="metric-label">Avg Gas / Block (7d)</div>
              <div className="metric-value text-accent-purple mt-1">
                {isLoading ? <span className="skeleton inline-block h-6 w-24" /> : formatGasUnits(data?.networkStats.avgGasPerBlock)}
              </div>
            </div>
          </div>
        </TuiPanel>
      </div>
    </div>
  );
}
