'use client';

import React from 'react';

interface LoadingStateProps {
  rows?: number;
  type?: 'table' | 'chart' | 'metrics';
}

export default function LoadingState({ rows = 5, type = 'table' }: LoadingStateProps) {
  if (type === 'chart') {
    return (
      <div className="flex items-end gap-1 h-[200px] p-4">
        {Array.from({ length: 20 }).map((_, i) => (
          <div
            key={i}
            className="skeleton flex-1"
            style={{ height: `${20 + Math.random() * 80}%`, animationDelay: `${i * 50}ms` }}
          />
        ))}
      </div>
    );
  }

  if (type === 'metrics') {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="tui-panel p-4">
            <div className="skeleton h-3 w-20 mb-3" />
            <div className="skeleton h-7 w-28" />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-3 p-4">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="skeleton h-8 w-full" style={{ animationDelay: `${i * 100}ms` }} />
      ))}
    </div>
  );
}
