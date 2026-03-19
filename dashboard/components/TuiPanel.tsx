'use client';

import React from 'react';

interface TuiPanelProps {
  title: string;
  children: React.ReactNode;
  rightContent?: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

export default function TuiPanel({ title, children, rightContent, className = '', noPadding = false }: TuiPanelProps) {
  return (
    <div className={`tui-panel ${className}`}>
      <div className="tui-panel-header">
        <span>{title}</span>
        {rightContent && <div className="flex items-center gap-2">{rightContent}</div>}
      </div>
      <div className={noPadding ? '' : 'p-4'}>
        {children}
      </div>
    </div>
  );
}
