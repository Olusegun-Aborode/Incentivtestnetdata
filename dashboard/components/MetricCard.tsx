'use client';

import React from 'react';

interface MetricCardProps {
  label: string;
  value: string | number;
  change?: string;
  changeType?: 'up' | 'down' | 'neutral';
  accent?: 'orange' | 'green' | 'blue' | 'purple' | 'cyan' | 'yellow' | 'red';
  loading?: boolean;
}

const accentColors: Record<string, string> = {
  orange: 'text-accent-orange',
  green: 'text-accent-green',
  blue: 'text-accent-blue',
  purple: 'text-accent-purple',
  cyan: 'text-accent-cyan',
  yellow: 'text-accent-yellow',
  red: 'text-accent-red',
};

export default function MetricCard({ label, value, change, changeType = 'neutral', accent = 'orange', loading = false }: MetricCardProps) {
  if (loading) {
    return (
      <div className="tui-panel">
        <div className="p-4">
          <div className="skeleton h-3 w-20 mb-3" />
          <div className="skeleton h-7 w-28" />
        </div>
      </div>
    );
  }

  return (
    <div className="tui-panel">
      <div className="p-4">
        <div className="metric-label">{label}</div>
        <div className={`metric-value mt-1 ${accentColors[accent]}`}>{value}</div>
        {change && (
          <div className={`text-xs mt-2 ${
            changeType === 'up' ? 'text-accent-green' :
            changeType === 'down' ? 'text-accent-red' :
            'text-text-muted'
          }`}>
            {changeType === 'up' ? '+' : changeType === 'down' ? '' : ''}{change}
          </div>
        )}
      </div>
    </div>
  );
}
