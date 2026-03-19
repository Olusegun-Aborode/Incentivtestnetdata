'use client';

import React, { useRef, useCallback, useState } from 'react';
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
  /** Time range buttons: if provided, renders 7D/30D/90D/180D pills */
  timeRange?: string;
  onTimeRangeChange?: (range: string) => void;
  /** Enable screenshot button */
  screenshotEnabled?: boolean;
  /** Enable expand/fullscreen button */
  expandEnabled?: boolean;
}

const CHART_COLORS = [
  '#E55A2B', '#4A6CF7', '#059669', '#9333EA', '#0891B2', '#D97706', '#DC2626',
];

const TIME_RANGES = ['7D', '30D', '90D', '180D'];

function CustomTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ name: string; value: number; color: string }>; label?: string }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-card border rounded-lg px-3 py-2 text-xs shadow-lg">
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

/** Camera icon for screenshot */
function CameraIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z" />
      <circle cx="12" cy="13" r="4" />
    </svg>
  );
}

/** Expand icon */
function ExpandIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="15 3 21 3 21 9" />
      <polyline points="9 21 3 21 3 15" />
      <line x1="21" y1="3" x2="14" y2="10" />
      <line x1="3" y1="21" x2="10" y2="14" />
    </svg>
  );
}

/** Collapse icon */
function CollapseIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="4 14 10 14 10 20" />
      <polyline points="20 10 14 10 14 4" />
      <line x1="14" y1="10" x2="21" y2="3" />
      <line x1="3" y1="21" x2="10" y2="14" />
    </svg>
  );
}

export default function ChartWrapper({
  data,
  type = 'area',
  xKey = 'date',
  yKey = 'value',
  yKeys,
  color = '#E55A2B',
  height = 250,
  loading = false,
  gradientId,
  pieDataKey = 'value',
  pieNameKey = 'name',
  timeRange,
  onTimeRangeChange,
  screenshotEnabled = true,
  expandEnabled = true,
}: ChartWrapperProps) {
  const chartRef = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState(false);
  const [screenshotting, setScreenshotting] = useState(false);

  const handleScreenshot = useCallback(async () => {
    if (!chartRef.current || screenshotting) return;
    setScreenshotting(true);
    try {
      // Dynamic import to avoid bundling html2canvas when not needed
      const html2canvas = (await import('html2canvas')).default;
      const canvas = await html2canvas(chartRef.current, {
        backgroundColor: '#ffffff',
        scale: 2,
      });
      const link = document.createElement('a');
      link.download = `chart-${new Date().toISOString().slice(0, 10)}.png`;
      link.href = canvas.toDataURL('image/png');
      link.click();
    } catch {
      // Fallback: use browser's built-in approach
      console.warn('Screenshot failed — html2canvas not available');
    } finally {
      setScreenshotting(false);
    }
  }, [screenshotting]);

  const handleExpand = useCallback(() => {
    setExpanded((prev) => !prev);
  }, []);

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
  const chartHeight = expanded ? '100%' : height;

  // Toolbar: time range pills + action buttons
  const toolbar = (
    <div className="flex items-center justify-between mb-2 px-1">
      <div className="flex items-center gap-1">
        {onTimeRangeChange && TIME_RANGES.map((r) => (
          <button
            key={r}
            onClick={() => onTimeRangeChange(r)}
            className={`time-pill ${timeRange === r ? 'active' : ''}`}
          >
            {r}
          </button>
        ))}
      </div>
      <div className="flex items-center gap-1">
        {screenshotEnabled && type !== 'pie' && (
          <button
            onClick={handleScreenshot}
            className="chart-action-btn"
            title="Download chart as image"
            disabled={screenshotting}
          >
            <CameraIcon />
          </button>
        )}
        {expandEnabled && type !== 'pie' && (
          <button
            onClick={handleExpand}
            className="chart-action-btn"
            title={expanded ? 'Collapse chart' : 'Expand chart'}
          >
            {expanded ? <CollapseIcon /> : <ExpandIcon />}
          </button>
        )}
      </div>
    </div>
  );

  const chartContent = (() => {
    if (type === 'pie') {
      return (
        <ResponsiveContainer width="100%" height={chartHeight}>
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
              stroke="#FFFFFF"
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
        <ResponsiveContainer width="100%" height={chartHeight}>
          <BarChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.06)" />
            <XAxis dataKey={xKey} tick={{ fontSize: 10 }} stroke="rgba(0,0,0,0.12)" />
            <YAxis tick={{ fontSize: 10 }} stroke="rgba(0,0,0,0.12)" tickFormatter={formatCompact} />
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
        <ResponsiveContainer width="100%" height={chartHeight}>
          <LineChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.06)" />
            <XAxis dataKey={xKey} tick={{ fontSize: 10 }} stroke="rgba(0,0,0,0.12)" />
            <YAxis tick={{ fontSize: 10 }} stroke="rgba(0,0,0,0.12)" tickFormatter={formatCompact} />
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
      <ResponsiveContainer width="100%" height={chartHeight}>
        <AreaChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
          <defs>
            <linearGradient id={gId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.3} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.06)" />
          <XAxis dataKey={xKey} tick={{ fontSize: 10 }} stroke="rgba(0,0,0,0.12)" />
          <YAxis tick={{ fontSize: 10 }} stroke="rgba(0,0,0,0.12)" tickFormatter={formatCompact} />
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
  })();

  // Expanded overlay mode
  if (expanded) {
    return (
      <>
        <div className="fixed inset-0 z-50 bg-black/30 backdrop-blur-sm" onClick={handleExpand} />
        <div className="fixed inset-4 z-50 bg-white rounded-xl shadow-2xl border border-gray-200 p-6 flex flex-col">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-1">
              {onTimeRangeChange && TIME_RANGES.map((r) => (
                <button
                  key={r}
                  onClick={() => onTimeRangeChange(r)}
                  className={`time-pill ${timeRange === r ? 'active' : ''}`}
                >
                  {r}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-1">
              {screenshotEnabled && (
                <button onClick={handleScreenshot} className="chart-action-btn" title="Download chart" disabled={screenshotting}>
                  <CameraIcon />
                </button>
              )}
              <button onClick={handleExpand} className="chart-action-btn" title="Collapse chart">
                <CollapseIcon />
              </button>
            </div>
          </div>
          <div ref={chartRef} className="flex-1">
            {chartContent}
          </div>
        </div>
      </>
    );
  }

  return (
    <div>
      {(onTimeRangeChange || screenshotEnabled || expandEnabled) && type !== 'pie' && toolbar}
      <div ref={chartRef}>
        {chartContent}
      </div>
    </div>
  );
}
