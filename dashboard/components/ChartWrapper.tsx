'use client';

import React from 'react';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import { formatCompact } from '@/lib/helpers';

interface ChartWrapperProps {
  data: Record<string, unknown>[];
  type?: 'area' | 'bar' | 'line' | 'pie';
  xKey?: string;
  yKey?: string;
  yKeys?: { key: string; color: string; name: string }[];
  color?: string;
  height?: number;
  loading?: boolean;
  gradientId?: string;
  pieDataKey?: string;
  pieNameKey?: string;
}

const CHART_COLORS = [
  '#FF6B35', '#5B7FFF', '#10B981', '#B44AFF', '#00D4FF', '#F59E0B', '#FF4444',
];

function CustomTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ name: string; value: number; color: string }>; label?: string }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-card border border-bright rounded px-3 py-2 text-xs font-mono">
      <div className="text-text-muted mb-1">{label}</div>
      {payload.map((entry, i) => (
        <div key={i} className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full" style={{ background: entry.color }} />
          <span className="text-foreground">{entry.name}: {formatCompact(entry.value)}</span>
        </div>
      ))}
    </div>
  );
}

export default function ChartWrapper({
  data,
  type = 'area',
  xKey = 'date',
  yKey = 'value',
  yKeys,
  color = '#FF6B35',
  height = 250,
  loading = false,
  gradientId,
  pieDataKey = 'value',
  pieNameKey = 'name',
}: ChartWrapperProps) {
  if (loading) {
    return (
      <div className="flex items-end gap-1 px-4" style={{ height }}>
        {Array.from({ length: 24 }).map((_, i) => (
          <div
            key={i}
            className="skeleton flex-1 rounded-t"
            style={{ height: `${15 + Math.random() * 85}%`, animationDelay: `${i * 40}ms` }}
          />
        ))}
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center text-text-muted text-sm" style={{ height }}>
        No data available
      </div>
    );
  }

  const gId = gradientId || `gradient-${Math.random().toString(36).slice(2)}`;

  if (type === 'pie') {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <PieChart>
          <Pie
            data={data}
            dataKey={pieDataKey}
            nameKey={pieNameKey}
            cx="50%"
            cy="50%"
            innerRadius={60}
            outerRadius={90}
            strokeWidth={1}
            stroke="#111318"
          >
            {data.map((_, index) => (
              <Cell key={index} fill={CHART_COLORS[index % CHART_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip content={<CustomTooltip />} />
        </PieChart>
      </ResponsiveContainer>
    );
  }

  if (type === 'bar') {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
          <XAxis dataKey={xKey} tick={{ fontSize: 10 }} stroke="rgba(255,255,255,0.1)" />
          <YAxis tick={{ fontSize: 10 }} stroke="rgba(255,255,255,0.1)" tickFormatter={formatCompact} />
          <Tooltip content={<CustomTooltip />} />
          {yKeys ? (
            yKeys.map((yk) => (
              <Bar key={yk.key} dataKey={yk.key} fill={yk.color} name={yk.name} radius={[2, 2, 0, 0]} />
            ))
          ) : (
            <Bar dataKey={yKey} fill={color} radius={[2, 2, 0, 0]} />
          )}
        </BarChart>
      </ResponsiveContainer>
    );
  }

  if (type === 'line') {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <LineChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
          <XAxis dataKey={xKey} tick={{ fontSize: 10 }} stroke="rgba(255,255,255,0.1)" />
          <YAxis tick={{ fontSize: 10 }} stroke="rgba(255,255,255,0.1)" tickFormatter={formatCompact} />
          <Tooltip content={<CustomTooltip />} />
          {yKeys ? (
            yKeys.map((yk) => (
              <Line key={yk.key} type="monotone" dataKey={yk.key} stroke={yk.color} name={yk.name} dot={false} strokeWidth={2} />
            ))
          ) : (
            <Line type="monotone" dataKey={yKey} stroke={color} dot={false} strokeWidth={2} />
          )}
        </LineChart>
      </ResponsiveContainer>
    );
  }

  // Default: area
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
        <defs>
          <linearGradient id={gId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.3} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
        <XAxis dataKey={xKey} tick={{ fontSize: 10 }} stroke="rgba(255,255,255,0.1)" />
        <YAxis tick={{ fontSize: 10 }} stroke="rgba(255,255,255,0.1)" tickFormatter={formatCompact} />
        <Tooltip content={<CustomTooltip />} />
        {yKeys ? (
          yKeys.map((yk, i) => {
            const gIdMulti = `${gId}-${i}`;
            return (
              <React.Fragment key={yk.key}>
                <defs>
                  <linearGradient id={gIdMulti} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={yk.color} stopOpacity={0.3} />
                    <stop offset="100%" stopColor={yk.color} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey={yk.key}
                  stroke={yk.color}
                  fill={`url(#${gIdMulti})`}
                  name={yk.name}
                  strokeWidth={2}
                />
              </React.Fragment>
            );
          })
        ) : (
          <Area type="monotone" dataKey={yKey} stroke={color} fill={`url(#${gId})`} strokeWidth={2} />
        )}
      </AreaChart>
    </ResponsiveContainer>
  );
}
