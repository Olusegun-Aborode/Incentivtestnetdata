'use client';

import React, { useState } from 'react';

interface TuiPanelProps {
  title: string;
  children: React.ReactNode;
  rightContent?: React.ReactNode;
  className?: string;
  noPadding?: boolean;
  /** Tooltip text explaining this panel */
  tooltip?: string;
}

function InfoIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="16" x2="12" y2="12" />
      <line x1="12" y1="8" x2="12.01" y2="8" />
    </svg>
  );
}

export default function TuiPanel({ title, children, rightContent, className = '', noPadding = false, tooltip }: TuiPanelProps) {
  const [showTooltip, setShowTooltip] = useState(false);

  return (
    <div className={`tui-panel ${className}`}>
      <div className="tui-panel-header">
        <span className="flex items-center gap-1.5">
          {title}
          {tooltip && (
            <span
              className="relative inline-flex"
              onMouseEnter={() => setShowTooltip(true)}
              onMouseLeave={() => setShowTooltip(false)}
            >
              <span className="text-text-muted hover:text-foreground cursor-help transition-colors">
                <InfoIcon />
              </span>
              {showTooltip && (
                <span className="panel-tooltip">
                  {tooltip}
                </span>
              )}
            </span>
          )}
        </span>
        {rightContent && <div className="flex items-center gap-2">{rightContent}</div>}
      </div>
      <div className={noPadding ? '' : 'p-4'}>
        {children}
      </div>
    </div>
  );
}
